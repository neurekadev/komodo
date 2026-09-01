use std::{
  collections::{HashMap, HashSet},
  sync::{Arc, Mutex, OnceLock},
  time::Duration,
};

use anyhow::{Context, anyhow};
use arc_swap::ArcSwap;
use async_timing_util::{
  get_timelength_in_ms, wait_until_timelength,
};
use futures_util::{StreamExt, stream};
use komodo_client::entities::{
  docker::{
    DockerMetricStatus, DockerNetworkUsage,
    container::{ContainerListItem, ContainerStateStatusEnum},
  },
  komodo_timestamp,
};

use crate::{config::periphery_config, state::docker_client};

use super::DockerClient;

const MAX_CONCURRENT_STATS_REQUESTS: usize = 8;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ContainerTrafficSample {
  network: String,
  ingress_bytes: u64,
  egress_bytes: u64,
  measured_at: i64,
}

type CollectedSamples =
  HashMap<String, Result<ContainerTrafficSample, String>>;

fn previous_samples()
-> &'static Mutex<HashMap<String, ContainerTrafficSample>> {
  static SAMPLES: OnceLock<
    Mutex<HashMap<String, ContainerTrafficSample>>,
  > = OnceLock::new();
  SAMPLES.get_or_init(Default::default)
}

fn network_usage_snapshot()
-> &'static ArcSwap<HashMap<String, DockerNetworkUsage>> {
  static SNAPSHOT: OnceLock<
    ArcSwap<HashMap<String, DockerNetworkUsage>>,
  > = OnceLock::new();
  SNAPSHOT.get_or_init(Default::default)
}

pub fn spawn_polling_thread() {
  tokio::spawn(async move {
    let polling_rate = periphery_config()
      .container_stats_polling_rate
      .to_string()
      .parse()
      .expect("invalid container stats polling rate");
    let stale_after_ms =
      (get_timelength_in_ms(polling_rate) * 2) as i64;
    refresh_network_usage(stale_after_ms).await;
    loop {
      let _ts = wait_until_timelength(polling_rate, 300).await;
      refresh_network_usage(stale_after_ms).await;
    }
  });
}

async fn refresh_network_usage(stale_after_ms: i64) {
  let client = docker_client().load();
  let Some(client) = client.iter().next() else {
    store_poll_failure("Docker is not connected");
    return;
  };
  let result = async {
    let containers_before = client.list_containers().await?;
    let samples = collect_samples(client, &containers_before).await;
    // Re-read membership after collecting counters. The later snapshot is
    // authoritative, and any change makes the earlier sample fail closed.
    let containers = client.list_containers().await?;
    let networks = client.list_networks(&containers).await?;
    let network_names = networks
      .into_iter()
      .filter_map(|network| network.name)
      .collect::<Vec<_>>();
    anyhow::Ok((containers, network_names, samples))
  }
  .await;
  let (containers, network_names, samples) = match result {
    Ok(result) => result,
    Err(e) => {
      error!("Failed to refresh Docker network usage | {e:#}");
      store_poll_failure("The latest Docker network poll failed");
      return;
    }
  };

  let mut previous = previous_samples()
    .lock()
    .expect("network sample mutex poisoned");
  let now = komodo_timestamp();
  let usage = calculate_network_usage(
    &network_names,
    &containers,
    &samples,
    &previous,
    now,
    stale_after_ms,
  );
  *previous = samples
    .into_iter()
    .filter_map(|(id, sample)| sample.ok().map(|sample| (id, sample)))
    .collect();
  network_usage_snapshot().store(Arc::new(usage));
}

async fn collect_samples(
  client: &DockerClient,
  containers: &[ContainerListItem],
) -> CollectedSamples {
  let targets = containers
    .iter()
    .filter_map(|container| {
      if container.state != ContainerStateStatusEnum::Running {
        return None;
      }
      let id = container.id.clone()?;
      let name = container.name.clone();
      let network =
        if container.network_mode.as_deref() == Some("host") {
          Err(
            "Host-network traffic cannot be attributed safely"
              .to_string(),
          )
        } else if container.networks.len() != 1 {
          Err(format!(
            "Container {name} is attached to {} named networks",
            container.networks.len()
          ))
        } else {
          Ok(container.networks[0].clone())
        };
      Some((id, name, network))
    })
    .collect::<Vec<_>>();

  stream::iter(targets.into_iter().map(
    |(id, name, network)| async move {
      let sample = match network {
        Ok(network) => client
          .network_counters_with_timeout(&name)
          .await
          .map(|(ingress_bytes, egress_bytes)| {
            ContainerTrafficSample {
              network,
              ingress_bytes,
              egress_bytes,
              measured_at: komodo_timestamp(),
            }
          })
          .map_err(|e| format!("{e:#}")),
        Err(reason) => Err(reason),
      };
      (id, sample)
    },
  ))
  .buffer_unordered(MAX_CONCURRENT_STATS_REQUESTS)
  .collect::<Vec<_>>()
  .await
  .into_iter()
  .collect()
}

impl DockerClient {
  async fn network_counters_with_timeout(
    &self,
    container_name: &str,
  ) -> anyhow::Result<(u64, u64)> {
    tokio::time::timeout(
      Duration::from_secs(10),
      self.network_counters(container_name),
    )
    .await
    .context("Timed out reading Docker container network counters")?
  }

  async fn network_counters(
    &self,
    container_name: &str,
  ) -> anyhow::Result<(u64, u64)> {
    let stats = self.full_container_stats(container_name).await?;
    let networks = stats.networks.context(
      "Docker omitted network counters from the container sample",
    )?;
    if networks.is_empty() {
      return Err(anyhow!(
        "Docker returned no network counters for the container"
      ));
    }
    networks.values().try_fold(
      (0_u64, 0_u64),
      |(ingress, egress), counters| {
        let rx = counters
          .rx_bytes
          .context("Docker omitted an ingress byte counter")?;
        let tx = counters
          .tx_bytes
          .context("Docker omitted an egress byte counter")?;
        Ok((
          ingress
            .checked_add(rx)
            .context("Docker ingress byte counters overflowed")?,
          egress
            .checked_add(tx)
            .context("Docker egress byte counters overflowed")?,
        ))
      },
    )
  }
}

fn calculate_network_usage(
  network_names: &[String],
  containers: &[ContainerListItem],
  current: &CollectedSamples,
  previous: &HashMap<String, ContainerTrafficSample>,
  now: i64,
  stale_after_ms: i64,
) -> HashMap<String, DockerNetworkUsage> {
  let network_names =
    network_names.iter().cloned().collect::<HashSet<_>>();
  network_names
    .into_iter()
    .map(|network| {
      let contributors = containers
        .iter()
        .filter(|container| {
          container.state == ContainerStateStatusEnum::Running
            && container.networks.contains(&network)
        })
        .collect::<Vec<_>>();
      let usage = calculate_one_network(
        &network,
        &contributors,
        current,
        previous,
        now,
        stale_after_ms,
      );
      (network, usage)
    })
    .collect()
}

fn calculate_one_network(
  network: &str,
  contributors: &[&ContainerListItem],
  current: &CollectedSamples,
  previous: &HashMap<String, ContainerTrafficSample>,
  now: i64,
  stale_after_ms: i64,
) -> DockerNetworkUsage {
  if contributors.is_empty() {
    return DockerNetworkUsage {
      status: DockerMetricStatus::Available,
      ingress_bytes: Some(0),
      egress_bytes: Some(0),
      measured_at: Some(now),
      rate_status: DockerMetricStatus::Available,
      ingress_bytes_per_second: Some(0.0),
      egress_bytes_per_second: Some(0.0),
      ..Default::default()
    };
  }

  let mut ingress = 0_u64;
  let mut egress = 0_u64;
  let mut ingress_rate = 0.0;
  let mut egress_rate = 0.0;
  let mut rate_reason = None;

  for container in contributors {
    if container.network_mode.as_deref() == Some("host") {
      return unavailable_network(
        now,
        "Host-network traffic cannot be attributed to a named network",
      );
    }
    if container.networks.len() != 1 {
      return unavailable_network(
        now,
        format!(
          "Container {} is attached to multiple named networks",
          container.name
        ),
      );
    }
    let Some(id) = container.id.as_deref() else {
      return unavailable_network(
        now,
        format!("Container {} has no Docker id", container.name),
      );
    };
    let sample = match current.get(id) {
      Some(Ok(sample)) => sample,
      Some(Err(reason)) => return unavailable_network(now, reason),
      None => {
        return unavailable_network(
          now,
          format!(
            "Container {} has no current sample",
            container.name
          ),
        );
      }
    };
    if sample.network != network {
      return unavailable_network(
        now,
        format!(
          "Container {} changed network membership during measurement",
          container.name
        ),
      );
    }
    if now.saturating_sub(sample.measured_at) > stale_after_ms {
      return unavailable_network(
        now,
        format!("Container {} has a stale sample", container.name),
      );
    }
    let Some(next_ingress) =
      ingress.checked_add(sample.ingress_bytes)
    else {
      return unavailable_network(
        now,
        "Ingress byte counters overflowed",
      );
    };
    ingress = next_ingress;
    let Some(next_egress) = egress.checked_add(sample.egress_bytes)
    else {
      return unavailable_network(
        now,
        "Egress byte counters overflowed",
      );
    };
    egress = next_egress;

    if rate_reason.is_some() {
      continue;
    }
    let Some(prior) = previous.get(id) else {
      rate_reason =
        Some("Waiting for a second stable sample".to_string());
      continue;
    };
    if prior.network != sample.network {
      rate_reason = Some(
        "Network membership changed since the previous sample"
          .to_string(),
      );
      continue;
    }
    if sample.ingress_bytes < prior.ingress_bytes
      || sample.egress_bytes < prior.egress_bytes
    {
      rate_reason = Some(
        "A container network counter reset since the previous sample"
          .to_string(),
      );
      continue;
    }
    let elapsed_ms = sample.measured_at - prior.measured_at;
    if elapsed_ms <= 0 {
      rate_reason = Some(
        "The previous sample is not older than the current sample"
          .to_string(),
      );
      continue;
    }
    let elapsed = elapsed_ms as f64 / 1_000.0;
    ingress_rate +=
      (sample.ingress_bytes - prior.ingress_bytes) as f64 / elapsed;
    egress_rate +=
      (sample.egress_bytes - prior.egress_bytes) as f64 / elapsed;
  }

  DockerNetworkUsage {
    status: DockerMetricStatus::Available,
    ingress_bytes: Some(ingress),
    egress_bytes: Some(egress),
    measured_at: Some(now),
    unavailable_reason: None,
    rate_status: if rate_reason.is_some() {
      DockerMetricStatus::Unavailable
    } else {
      DockerMetricStatus::Available
    },
    ingress_bytes_per_second: rate_reason
      .is_none()
      .then_some(ingress_rate),
    egress_bytes_per_second: rate_reason
      .is_none()
      .then_some(egress_rate),
    rate_unavailable_reason: rate_reason,
  }
}

fn unavailable_network(
  measured_at: i64,
  reason: impl Into<String>,
) -> DockerNetworkUsage {
  let reason = reason.into();
  DockerNetworkUsage {
    status: DockerMetricStatus::Unavailable,
    measured_at: Some(measured_at),
    unavailable_reason: Some(reason.clone()),
    rate_status: DockerMetricStatus::Unavailable,
    rate_unavailable_reason: Some(reason),
    ..Default::default()
  }
}

fn store_poll_failure(reason: &str) {
  let measured_at = komodo_timestamp();
  let snapshot = network_usage_snapshot().load();
  let unavailable = snapshot
    .keys()
    .map(|name| {
      (
        name.clone(),
        unavailable_network(measured_at, reason.to_string()),
      )
    })
    .collect();
  network_usage_snapshot().store(Arc::new(unavailable));
}

pub fn network_usage(network_name: &str) -> DockerNetworkUsage {
  network_usage_snapshot()
    .load()
    .get(network_name)
    .cloned()
    .unwrap_or_default()
}

#[cfg(test)]
mod tests {
  use super::*;

  fn container(id: &str, networks: &[&str]) -> ContainerListItem {
    ContainerListItem {
      id: Some(id.to_string()),
      name: id.to_string(),
      state: ContainerStateStatusEnum::Running,
      networks: networks
        .iter()
        .map(|name| name.to_string())
        .collect(),
      ..Default::default()
    }
  }

  fn sample(
    network: &str,
    ingress: u64,
    egress: u64,
    measured_at: i64,
  ) -> Result<ContainerTrafficSample, String> {
    Ok(ContainerTrafficSample {
      network: network.to_string(),
      ingress_bytes: ingress,
      egress_bytes: egress,
      measured_at,
    })
  }

  #[test]
  fn attributes_complete_single_network_totals_and_rates() {
    let containers =
      [container("one", &["sites"]), container("two", &["sites"])];
    let current = HashMap::from([
      ("one".into(), sample("sites", 300, 500, 2_000)),
      ("two".into(), sample("sites", 700, 900, 2_000)),
    ]);
    let previous = HashMap::from([
      ("one".into(), sample("sites", 100, 200, 1_000).unwrap()),
      ("two".into(), sample("sites", 400, 500, 1_000).unwrap()),
    ]);
    let usage = calculate_one_network(
      "sites",
      &containers.iter().collect::<Vec<_>>(),
      &current,
      &previous,
      2_000,
      60_000,
    );
    assert_eq!(usage.status, DockerMetricStatus::Available);
    assert_eq!(usage.ingress_bytes, Some(1_000));
    assert_eq!(usage.egress_bytes, Some(1_400));
    assert_eq!(usage.ingress_bytes_per_second, Some(500.0));
    assert_eq!(usage.egress_bytes_per_second, Some(700.0));
  }

  #[test]
  fn rejects_multi_network_attribution() {
    let containers = [container("one", &["sites", "database"])];
    let usage = calculate_one_network(
      "sites",
      &containers.iter().collect::<Vec<_>>(),
      &HashMap::new(),
      &HashMap::new(),
      2_000,
      60_000,
    );
    assert_eq!(usage.status, DockerMetricStatus::Unavailable);
    assert!(usage.unavailable_reason.unwrap().contains("multiple"));
  }

  #[test]
  fn rejects_missing_and_stale_samples() {
    let containers = [container("one", &["sites"])];
    let refs = containers.iter().collect::<Vec<_>>();
    let missing = calculate_one_network(
      "sites",
      &refs,
      &HashMap::new(),
      &HashMap::new(),
      100_000,
      60_000,
    );
    assert!(
      missing.unavailable_reason.unwrap().contains("no current")
    );

    let stale = calculate_one_network(
      "sites",
      &refs,
      &HashMap::from([("one".into(), sample("sites", 1, 1, 1_000))]),
      &HashMap::new(),
      100_000,
      60_000,
    );
    assert!(stale.unavailable_reason.unwrap().contains("stale"));
  }

  #[test]
  fn counter_reset_and_membership_change_suppress_rates() {
    let containers = [container("one", &["sites"])];
    let refs = containers.iter().collect::<Vec<_>>();
    let current =
      HashMap::from([("one".into(), sample("sites", 10, 10, 2_000))]);
    let reset = calculate_one_network(
      "sites",
      &refs,
      &current,
      &HashMap::from([(
        "one".into(),
        sample("sites", 20, 20, 1_000).unwrap(),
      )]),
      2_000,
      60_000,
    );
    assert_eq!(reset.status, DockerMetricStatus::Available);
    assert_eq!(reset.rate_status, DockerMetricStatus::Unavailable);
    assert!(reset.rate_unavailable_reason.unwrap().contains("reset"));

    let changed = calculate_one_network(
      "sites",
      &refs,
      &current,
      &HashMap::from([(
        "one".into(),
        sample("old", 1, 1, 1_000).unwrap(),
      )]),
      2_000,
      60_000,
    );
    assert!(
      changed
        .rate_unavailable_reason
        .unwrap()
        .contains("membership")
    );

    let changed_during_poll = calculate_one_network(
      "sites",
      &refs,
      &HashMap::from([("one".into(), sample("old", 10, 10, 2_000))]),
      &HashMap::new(),
      2_000,
      60_000,
    );
    assert_eq!(
      changed_during_poll.status,
      DockerMetricStatus::Unavailable
    );
    assert!(
      changed_during_poll
        .unavailable_reason
        .unwrap()
        .contains("during measurement")
    );
  }
}
