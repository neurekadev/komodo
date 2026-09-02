use std::{collections::HashMap, sync::Arc, time::Duration};

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
  image_unavailable_reason: Option<String>,
  volume_unavailable_reason: Option<String>,
}

const DISK_USAGE_TIMEOUT: Duration = Duration::from_secs(120);

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
    store_initial_failure(
      disk_usage_snapshot(),
      "Docker is not connected",
    );
    warn!(
      "Unable to refresh Docker disk usage: Docker is not connected"
    );
    return;
  };
  if let Err(e) = store_snapshot_result(
    disk_usage_snapshot(),
    client.disk_usage_snapshot().await,
  ) {
    // A transient daemon failure must not erase the last good snapshot.
    error!("Failed to refresh Docker disk usage cache | {e:#}");
  }
}

fn store_snapshot_result(
  target: &ArcSwap<DockerDiskUsageSnapshot>,
  result: anyhow::Result<DockerDiskUsageSnapshot>,
) -> anyhow::Result<()> {
  match result {
    Ok(snapshot) => {
      target.store(Arc::new(snapshot));
      Ok(())
    }
    Err(error) => {
      store_initial_failure(
        target,
        "The initial Docker disk usage measurement failed",
      );
      Err(error)
    }
  }
}

fn store_initial_failure(
  target: &ArcSwap<DockerDiskUsageSnapshot>,
  reason: &str,
) {
  if target.load().measured_at != 0 {
    return;
  }
  target.store(Arc::new(DockerDiskUsageSnapshot {
    measured_at: komodo_timestamp(),
    image_unavailable_reason: Some(reason.to_string()),
    volume_unavailable_reason: Some(reason.to_string()),
    ..Default::default()
  }));
}

impl DockerClient {
  async fn disk_usage_snapshot(
    &self,
  ) -> anyhow::Result<DockerDiskUsageSnapshot> {
    let response = tokio::time::timeout(
      DISK_USAGE_TIMEOUT,
      self.docker.df(
        DataUsageOptionsBuilder::new()
          ._type(vec!["image".to_string(), "volume".to_string()])
          .verbose(true)
          .build()
          .into(),
      ),
    )
    .await
    .context("Timed out measuring Docker disk usage")??;
    Ok(parse_disk_usage(response, komodo_timestamp()))
  }
}

fn parse_disk_usage(
  response: SystemDataUsageResponse,
  measured_at: i64,
) -> DockerDiskUsageSnapshot {
  let (images, image_unavailable_reason) =
    match response.image_usage.and_then(|usage| usage.items) {
      Some(items) => parse_image_usage(items, measured_at),
      None => (
        HashMap::new(),
        Some("Docker omitted image disk usage".to_string()),
      ),
    };
  let (volumes, volume_unavailable_reason) =
    match response.volume_usage.and_then(|usage| usage.items) {
      Some(items) => parse_volume_usage(items, measured_at),
      None => (
        HashMap::new(),
        Some("Docker omitted volume disk usage".to_string()),
      ),
    };

  DockerDiskUsageSnapshot {
    measured_at,
    images,
    volumes,
    image_unavailable_reason,
    volume_unavailable_reason,
  }
}

fn parse_image_usage(
  items: Vec<serde_json::Value>,
  measured_at: i64,
) -> (HashMap<String, ImageDiskUsage>, Option<String>) {
  let mut images = HashMap::new();
  let mut unavailable_reason = None;
  for item in items {
    let image: ImageSummary = match serde_json::from_value(item) {
      Ok(image) => image,
      Err(error) => {
        warn!(
          "Failed to decode an image disk usage entry | {error:#}"
        );
        unavailable_reason = Some(
          "Docker returned an invalid image disk usage entry"
            .to_string(),
        );
        continue;
      }
    };
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
    images.insert(image.id, usage);
  }
  (images, unavailable_reason)
}

fn parse_volume_usage(
  items: Vec<serde_json::Value>,
  measured_at: i64,
) -> (HashMap<String, VolumeDiskUsage>, Option<String>) {
  let mut volumes = HashMap::new();
  let mut unavailable_reason = None;
  for item in items {
    let volume: Volume = match serde_json::from_value(item) {
      Ok(volume) => volume,
      Err(error) => {
        warn!(
          "Failed to decode a volume disk usage entry | {error:#}"
        );
        unavailable_reason = Some(
          "Docker returned an invalid volume disk usage entry"
            .to_string(),
        );
        continue;
      }
    };
    let usage = volume_usage(
      &volume.driver,
      volume.usage_data.map(|usage| usage.size),
      measured_at,
    );
    volumes.insert(volume.name, usage);
  }
  (volumes, unavailable_reason)
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
      snapshot.image_unavailable_reason.as_deref().unwrap_or(
        "Image was not present in the latest Docker disk usage snapshot",
      ),
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
      snapshot
        .volume_unavailable_reason
        .as_deref()
        .map(|reason| VolumeDiskUsage {
          status: DockerMetricStatus::Unavailable,
          measured_at: Some(snapshot.measured_at),
          unavailable_reason: Some(reason.to_string()),
          ..Default::default()
        })
        .unwrap_or_else(|| {
          volume_usage(driver, None, snapshot.measured_at)
        })
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
    );
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
    );
    let usage = &snapshot.volumes["remote"];
    assert_eq!(usage.status, DockerMetricStatus::Unavailable);
    assert_eq!(usage.measured_at, Some(456));
    assert!(
      usage.unavailable_reason.as_deref().unwrap().contains("nfs")
    );
  }

  #[test]
  fn keeps_volume_usage_when_image_usage_is_missing() {
    let snapshot = parse_disk_usage(
      SystemDataUsageResponse {
        image_usage: None,
        volume_usage: Some(VolumesDiskUsage {
          items: Some(vec![json!({
            "Name": "data",
            "Driver": "local",
            "Mountpoint": "/var/lib/docker/volumes/data/_data",
            "Scope": "local",
            "Labels": {},
            "Options": {},
            "UsageData": { "Size": 42, "RefCount": 1 }
          })]),
          ..Default::default()
        }),
        ..Default::default()
      },
      789,
    );
    assert_eq!(
      snapshot.volumes["data"],
      VolumeDiskUsage {
        status: DockerMetricStatus::Available,
        used_bytes: Some(42),
        measured_at: Some(789),
        unavailable_reason: None,
      }
    );
    assert!(snapshot.image_unavailable_reason.is_some());
    assert!(snapshot.volume_unavailable_reason.is_none());
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
      image_unavailable_reason: None,
      volume_unavailable_reason: None,
    };
    let target = ArcSwap::from_pointee(initial.clone());
    assert!(
      store_snapshot_result(
        &target,
        Err(anyhow::anyhow!("transient Docker error")),
      )
      .is_err()
    );
    assert_eq!(target.load().as_ref(), &initial);
  }

  #[test]
  fn failed_initial_refresh_stops_reporting_pending() {
    let target =
      ArcSwap::from_pointee(DockerDiskUsageSnapshot::default());
    assert!(
      store_snapshot_result(
        &target,
        Err(anyhow::anyhow!("Docker request failed")),
      )
      .is_err()
    );
    let snapshot = target.load();
    assert_ne!(snapshot.measured_at, 0);
    assert!(snapshot.image_unavailable_reason.is_some());
    assert!(snapshot.volume_unavailable_reason.is_some());
  }
}
