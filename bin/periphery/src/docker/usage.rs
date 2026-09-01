use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use arc_swap::ArcSwap;
use async_timing_util::wait_until_timelength;
use bollard::models::{
  ImageSummary, SystemDataUsageResponse, Volume,
};
use bollard::query_parameters::DataUsageOptionsBuilder;
use komodo_client::entities::{
  docker::{DockerMetricStatus, ImageDiskUsage, VolumeDiskUsage},
  komodo_timestamp,
};
use std::sync::OnceLock;

use crate::{config::periphery_config, state::docker_client};

use super::DockerClient;

#[derive(Debug, Clone, Default, PartialEq)]
struct DockerDiskUsageSnapshot {
  measured_at: i64,
  images: HashMap<String, ImageDiskUsage>,
  volumes: HashMap<String, VolumeDiskUsage>,
}

fn disk_usage_snapshot() -> &'static ArcSwap<DockerDiskUsageSnapshot>
{
  static SNAPSHOT: OnceLock<ArcSwap<DockerDiskUsageSnapshot>> =
    OnceLock::new();
  SNAPSHOT.get_or_init(Default::default)
}

pub fn spawn_polling_thread() {
  tokio::spawn(async move {
    let polling_rate = periphery_config()
      .docker_disk_usage_polling_rate
      .to_string()
      .parse()
      .expect("invalid Docker disk usage polling rate");
    refresh_disk_usage().await;
    loop {
      let _ts = wait_until_timelength(polling_rate, 400).await;
      refresh_disk_usage().await;
    }
  });
}

async fn refresh_disk_usage() {
  let client = docker_client().load();
  let Some(client) = client.iter().next() else {
    warn!(
      "Unable to refresh Docker disk usage: Docker is not connected"
    );
    return;
  };
  if let Err(e) = store_successful_snapshot(
    disk_usage_snapshot(),
    client.disk_usage_snapshot().await,
  ) {
    // A transient daemon failure must not erase the last good snapshot.
    error!("Failed to refresh Docker disk usage cache | {e:#}");
  }
}

fn store_successful_snapshot(
  target: &ArcSwap<DockerDiskUsageSnapshot>,
  result: anyhow::Result<DockerDiskUsageSnapshot>,
) -> anyhow::Result<()> {
  let snapshot = result?;
  target.store(Arc::new(snapshot));
  Ok(())
}

impl DockerClient {
  async fn disk_usage_snapshot(
    &self,
  ) -> anyhow::Result<DockerDiskUsageSnapshot> {
    let response = self
      .docker
      .df(
        DataUsageOptionsBuilder::new()
          ._type(vec!["image".to_string(), "volume".to_string()])
          .verbose(true)
          .build()
          .into(),
      )
      .await?;
    parse_disk_usage(response, komodo_timestamp())
  }
}

fn parse_disk_usage(
  response: SystemDataUsageResponse,
  measured_at: i64,
) -> anyhow::Result<DockerDiskUsageSnapshot> {
  let image_items = response
    .image_usage
    .context("Docker disk usage omitted image usage")?
    .items
    .context("Docker disk usage omitted image items")?;
  let volume_items = response
    .volume_usage
    .context("Docker disk usage omitted volume usage")?
    .items
    .context("Docker disk usage omitted volume items")?;

  let images = image_items
    .into_iter()
    .map(|item| {
      let image: ImageSummary = serde_json::from_value(item)
        .context("Failed to decode an image disk usage entry")?;
      let usage = if image.size < 0 || image.shared_size < 0 {
        unavailable_image(
          measured_at,
          "Docker did not report complete image layer usage",
        )
      } else if image.shared_size > image.size {
        unavailable_image(
          measured_at,
          "Docker reported shared image usage larger than total usage",
        )
      } else {
        ImageDiskUsage {
          status: DockerMetricStatus::Available,
          total_bytes: Some(image.size),
          shared_bytes: Some(image.shared_size),
          unique_bytes: Some(image.size - image.shared_size),
          measured_at: Some(measured_at),
          unavailable_reason: None,
        }
      };
      anyhow::Ok((image.id, usage))
    })
    .collect::<anyhow::Result<HashMap<_, _>>>()?;

  let volumes = volume_items
    .into_iter()
    .map(|item| {
      let volume: Volume = serde_json::from_value(item)
        .context("Failed to decode a volume disk usage entry")?;
      let usage = volume_usage(
        &volume.driver,
        volume.usage_data.map(|usage| usage.size),
        measured_at,
      );
      anyhow::Ok((volume.name, usage))
    })
    .collect::<anyhow::Result<HashMap<_, _>>>()?;

  Ok(DockerDiskUsageSnapshot {
    measured_at,
    images,
    volumes,
  })
}

fn unavailable_image(
  measured_at: i64,
  reason: impl Into<String>,
) -> ImageDiskUsage {
  ImageDiskUsage {
    status: DockerMetricStatus::Unavailable,
    measured_at: Some(measured_at),
    unavailable_reason: Some(reason.into()),
    ..Default::default()
  }
}

fn volume_usage(
  driver: &str,
  used_bytes: Option<i64>,
  measured_at: i64,
) -> VolumeDiskUsage {
  if driver != "local" {
    return VolumeDiskUsage {
      status: DockerMetricStatus::Unavailable,
      measured_at: Some(measured_at),
      unavailable_reason: Some(format!(
        "The {driver} volume driver does not report local disk usage"
      )),
      ..Default::default()
    };
  }
  match used_bytes {
    Some(used_bytes) if used_bytes >= 0 => VolumeDiskUsage {
      status: DockerMetricStatus::Available,
      used_bytes: Some(used_bytes),
      measured_at: Some(measured_at),
      unavailable_reason: None,
    },
    _ => VolumeDiskUsage {
      status: DockerMetricStatus::Unavailable,
      measured_at: Some(measured_at),
      unavailable_reason: Some(
        "Docker did not report usage for this local volume"
          .to_string(),
      ),
      ..Default::default()
    },
  }
}

pub fn image_disk_usage(image_id: &str) -> ImageDiskUsage {
  let snapshot = disk_usage_snapshot().load();
  if snapshot.measured_at == 0 {
    return Default::default();
  }
  snapshot.images.get(image_id).cloned().unwrap_or_else(|| {
    unavailable_image(
      snapshot.measured_at,
      "Image was not present in the latest Docker disk usage snapshot",
    )
  })
}

pub fn volume_disk_usage(
  volume_name: &str,
  driver: &str,
) -> VolumeDiskUsage {
  let snapshot = disk_usage_snapshot().load();
  if snapshot.measured_at == 0 {
    return Default::default();
  }
  snapshot
    .volumes
    .get(volume_name)
    .cloned()
    .unwrap_or_else(|| {
      volume_usage(driver, None, snapshot.measured_at)
    })
}

#[cfg(test)]
mod tests {
  use bollard::models::{ImagesDiskUsage, VolumesDiskUsage};
  use serde_json::json;

  use super::*;

  fn response(
    images: Vec<serde_json::Value>,
    volumes: Vec<serde_json::Value>,
  ) -> SystemDataUsageResponse {
    SystemDataUsageResponse {
      image_usage: Some(ImagesDiskUsage {
        items: Some(images),
        ..Default::default()
      }),
      volume_usage: Some(VolumesDiskUsage {
        items: Some(volumes),
        ..Default::default()
      }),
      ..Default::default()
    }
  }

  #[test]
  fn maps_image_total_shared_and_unique_usage() {
    let snapshot = parse_disk_usage(
      response(
        vec![json!({
          "Id": "sha256:one",
          "ParentId": "",
          "RepoTags": [],
          "RepoDigests": [],
          "Created": 1,
          "Size": 1000,
          "SharedSize": 400,
          "Labels": {},
          "Containers": 0
        })],
        vec![],
      ),
      123,
    )
    .unwrap();
    assert_eq!(
      snapshot.images["sha256:one"],
      ImageDiskUsage {
        status: DockerMetricStatus::Available,
        total_bytes: Some(1000),
        shared_bytes: Some(400),
        unique_bytes: Some(600),
        measured_at: Some(123),
        unavailable_reason: None,
      }
    );
  }

  #[test]
  fn marks_unsupported_volume_driver_unavailable() {
    let snapshot = parse_disk_usage(
      response(
        vec![],
        vec![json!({
          "Name": "remote",
          "Driver": "nfs",
          "Mountpoint": "/mnt/remote",
          "Scope": "local",
          "Labels": {},
          "Options": {},
          "UsageData": { "Size": 10, "RefCount": 0 }
        })],
      ),
      456,
    )
    .unwrap();
    let usage = &snapshot.volumes["remote"];
    assert_eq!(usage.status, DockerMetricStatus::Unavailable);
    assert_eq!(usage.measured_at, Some(456));
    assert!(
      usage.unavailable_reason.as_deref().unwrap().contains("nfs")
    );
  }

  #[test]
  fn rejects_partial_disk_usage_response() {
    let response = SystemDataUsageResponse {
      image_usage: Some(ImagesDiskUsage {
        items: Some(vec![]),
        ..Default::default()
      }),
      volume_usage: None,
      ..Default::default()
    };
    assert!(parse_disk_usage(response, 1).is_err());
  }

  #[test]
  fn failed_refresh_retains_last_successful_snapshot() {
    let initial = DockerDiskUsageSnapshot {
      measured_at: 123,
      images: HashMap::from([(
        "sha256:one".to_string(),
        ImageDiskUsage {
          status: DockerMetricStatus::Available,
          total_bytes: Some(10),
          ..Default::default()
        },
      )]),
      volumes: Default::default(),
    };
    let target = ArcSwap::from_pointee(initial.clone());
    assert!(
      store_successful_snapshot(
        &target,
        Err(anyhow::anyhow!("transient Docker error")),
      )
      .is_err()
    );
    assert_eq!(target.load().as_ref(), &initial);
  }
}
