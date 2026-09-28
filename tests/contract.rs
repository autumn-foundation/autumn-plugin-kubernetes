//! The fake passes the same contract as the real API server (AC13).
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

mod common;

use autumn_plugin_kubernetes::api::MemoryKubeApi;
use common::contract;

#[tokio::test]
async fn memory_passes_lease_contract() {
    contract::lease_contract(&MemoryKubeApi::new(), "ns", "l").await;
}

#[tokio::test]
async fn memory_passes_config_map_contract() {
    let api = MemoryKubeApi::new();
    let (a, b) = (api.clone(), api.clone());
    contract::config_map_contract(
        &api,
        "ns",
        "flags",
        async move |name, data| a.put_config_map("ns", name, data),
        async move |name| b.delete_config_map("ns", name),
    )
    .await;
}

#[tokio::test]
async fn memory_passes_event_contract() {
    let api = MemoryKubeApi::new();
    contract::event_contract(&api, "ns", "p").await;
    assert_eq!(api.events().len(), 1);
}
