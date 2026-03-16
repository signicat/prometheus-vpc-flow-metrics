# prometheus-vpc-flow-metrics

Tool that streams GCP VPC Flow log log messages from PubSub and exposes Prometheus metrics.

Useful for mapping especially which pods are engaged in Inter-zone network traffic (which is costly at scale).

> **PS**: `prometheus-vpc-flow-metrics` can ingest VPC flow logs from multiple GCP projects and GKE clusters. You do not need to host separate instances per project/cluster.

## Quick Start

This assumes you are going to run `prometheus-vpc-flow-metrics` as a Kubernetes deployment on GKE. And are interested in VPC Flow Logs for GKE clusters.

### Prerequisites

- Enable [VPC Flow Logs](https://docs.cloud.google.com/vpc/docs/flow-logs) for the GKE subnets.
- Create a [Pub/Sub topic](https://docs.cloud.google.com/pubsub/docs/create-topic) for the flow logs.
- Configure a [Cloud Logging Sink for routing](https://docs.cloud.google.com/logging/docs/routing/overview) the flow logs into the Pub/Sub topic.

[Here is a good tutorial on these steps from netography.com](https://docs.netography.com/ingest-network-traffic-logs/flow-logs/gcp-flow-logs-via-pubsub). **Skip steps 4 (Pub/Sub Pull Subscription) and 5 (granting access to netography).**

- Create a Google Service Account with access to the Pub/Sub topic
- Set up [Workload Identity Federation](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/workload-identity) to grant access to the (Kubernetes) ServiceAccount `prometheus-vpc-flow-metrics` is running as to the Google Service Account created above.

### Installation

This required you to have a the GCP Project ID and Subscription ID for the PubSub topic where VPC flow logs are published.

    helm repo add prometheus-vpc-flow-metrics 'https://raw.githubusercontent.com/signicat/prometheus-vpc-flow-metrics/main/chart/'
    helm repo update

    kubectl create namespace prometheus-vpc-flow-metrics
    helm upgrade --install --namespace=prometheus-vpc-flow-metrics prometheus-vpc-flow-metrics \
        prometheus-vpc-flow-metrics/prometheus-vpc-flow-metrics \
        --set env.PROMETHEUS_VPC_FLOW_METRICS_PUBSUB_PROJECT_ID=xxx \
        --set env.PROMETHEUS_VPC_FLOW_METRICS_PUBSUB_SUBSCRIPTION_ID=xxx

## Configuration

### Aggregation Interval

> https://docs.cloud.google.com/vpc/docs/flow-logs#log-sampling

With 30s Aggregation Interval I have good success with these settings (which are the defaults):

    PROMETHEUS_VPC_FLOW_METRICS_METRICS_CACHE_LIFETIME: 5m
    PROMETHEUS_VPC_FLOW_METRICS_MAX_MESSAGE_AGE: 10s

We recently changed to 5m Aggregation Interval and adjusted accordingly:

    PROMETHEUS_VPC_FLOW_METRICS_METRICS_CACHE_LIFETIME: 15m
    PROMETHEUS_VPC_FLOW_METRICS_MAX_MESSAGE_AGE: 1m

### Other settings

`PROMETHEUS_VPC_FLOW_METRICS_ENABLE_PACKET_METRICS` - Export packet counts in addition to bandwidth. Default off. Set to `true` to enable.
`PROMETHEUS_VPC_FLOW_METRICS_ENABLE_MESSAGES_IGNORED_DESTINATION_MISSING_METRIC` - Enable an additional internal metric of ignored messages due to missing `destination` in the VPC flow events that _also_ includes per-flow labels. Useful for debugging. Totally overkill for production. Aggregate counts already included in `vpcflow_exporter_pubsub_messages_ignored{reason="destination_missing"}` metric. Default off.

# Wishlist
 - Update pubsub library to v2
 - Config for enabling/disabling logging
 - Add configurable OmitLabels (for potentially redundant things like src_vpc_name etc)
 - Add optional logging of destination missing flows, maybe with DNS reverse lookup? Could be useful to find unknown traffic paths outside the cluster
