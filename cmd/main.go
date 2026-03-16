package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/VictoriaMetrics/metrics"
)

var (
	Version   string
	Revision  string
	Branch    string
	BuildTime string
)

func main() {
	log.Printf("Starting prometheus-vpc-flow-metrics. Version: %s, Revision: %s, Branch: %s, BuildTime: %s.\n", Version, Revision, Branch, BuildTime)

	ctx := context.Background()

	projectID := os.Getenv("PROMETHEUS_VPC_FLOW_METRICS_PUBSUB_PROJECT_ID")
	if projectID == "" {
		log.Fatalf("Environment variable PROMETHEUS_VPC_FLOW_METRICS_PUBSUB_PROJECT_ID must be set. Quitting.")
	}
	subscriptionID := os.Getenv("PROMETHEUS_VPC_FLOW_METRICS_PUBSUB_SUBSCRIPTION_ID")
	if subscriptionID == "" {
		log.Fatalf("Environment variable PROMETHEUS_VPC_FLOW_METRICS_PUBSUB_SUBSCRIPTION_ID must be set. Quitting.")
	}

	metricsCacheLifetime := time.Minute * 5
	if os.Getenv("PROMETHEUS_VPC_FLOW_METRICS_METRICS_CACHE_LIFETIME") != "" {
		var err error
		metricsCacheLifetime, err = time.ParseDuration(os.Getenv("PROMETHEUS_VPC_FLOW_METRICS_METRICS_CACHE_LIFETIME"))
		if err != nil {
			log.Fatalf("Could not parse PROMETHEUS_VPC_FLOW_METRICS_METRICS_CACHE_LIFETIME into duration: %v", err)
		}
	}

	maxMessageAge := time.Second * 10
	if os.Getenv("PROMETHEUS_VPC_FLOW_METRICS_MAX_MESSAGE_AGE") != "" {
		var err error
		maxMessageAge, err = time.ParseDuration(os.Getenv("PROMETHEUS_VPC_FLOW_METRICS_MAX_MESSAGE_AGE"))
		if err != nil {
			log.Fatalf("Could not parse PROMETHEUS_VPC_FLOW_METRICS_MAX_MESSAGE_AGE into duration: %v", err)
		}
	}

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}

	sub := client.Subscription(subscriptionID)

	// Run HTTP server for Prometheus metrics
	go func() {
		http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			metrics.WritePrometheus(w, true) // `true` = include comments
		})
		log.Println("serving metrics on :8080/metrics")
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	lastFlowReceivedTimes := make(map[string]time.Time)
	var lastFlowReceivedTimesMutex sync.RWMutex

	metrics.GetOrCreateGauge(
		fmt.Sprintf(`vpcflow_exporter_metrics_active{project="%s",subscription="%s"}`, projectID, subscriptionID),
		func() float64 {
			return float64(len(metrics.ListMetricNames()))
		})

	// Goroutine for cleaning up metrics for flows that haven't been seen in a while
	go func() {
		for {
			time.Sleep(time.Second * 30)

			log.Println("Removing old flow metrics")

			metricsRemoved := 0
			lastFlowReceivedTimesMutex.Lock()
			for metricLabels, flowTimestamp := range lastFlowReceivedTimes {
				if time.Since(flowTimestamp) > metricsCacheLifetime {
					metrics.UnregisterMetric(fmt.Sprintf("vpcflow_bytes_total{%s}", metricLabels))
					metrics.UnregisterMetric(fmt.Sprintf("vpcflow_packets_total{%s}", metricLabels))

					delete(lastFlowReceivedTimes, metricLabels)
					metricsRemoved++
				}
			}

			metrics.GetOrCreateCounter(
				fmt.Sprintf(`vpcflow_exporter_metrics_unregistered{project="%s",subscription="%s"}`, projectID, subscriptionID),
			).Add(metricsRemoved)
			log.Printf("Removed %d old flow metrics", metricsRemoved)

			lastFlowReceivedTimesMutex.Unlock()
		}
	}()

	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		// Always Ack messages regardless of success or not to avoid clogging up the subscription
		defer m.Ack()

		metrics.GetOrCreateCounter(
			fmt.Sprintf(`vpcflow_exporter_pubsub_messages_received{project="%s",subscription="%s"}`, projectID, subscriptionID),
		).Inc()

		var entry LogEntry
		if err := json.Unmarshal(m.Data, &entry); err != nil {
			log.Printf("failed to parse message: %v", err)
			metrics.GetOrCreateCounter(
				fmt.Sprintf(`vpcflow_exporter_pubsub_messages_failed{project="%s",subscription="%s", reason="error_unmarshal_json"}`, projectID, subscriptionID),
			).Inc()
			return
		} else {
			//log.Printf("Flow: %s:%d -> %s:%d bytes=%s packets=%s",
			//	entry.JSONPayload.Connection.SrcIP,
			//	entry.JSONPayload.Connection.SrcPort,
			//	entry.JSONPayload.Connection.DestIP,
			//	entry.JSONPayload.Connection.DestPort,
			//	entry.JSONPayload.BytesSent,
			//	entry.JSONPayload.PacketsSent,
			//)
		}

		if entry.JSONPayload.Reporter != "SRC" {
			metrics.GetOrCreateCounter(
				fmt.Sprintf(`vpcflow_exporter_pubsub_messages_ignored{project="%s",subscription="%s", reason="reporter_not_src"}`, projectID, subscriptionID),
			).Inc()
			return
		}

		if time.Since(entry.Timestamp) > maxMessageAge {
			log.Printf("Ignoring message with age %v (older than %v). Consider increasing PROMETHEUS_VPC_FLOW_METRICS_MAX_MESSAGE_AGE", time.Since(entry.Timestamp), maxMessageAge)
			metrics.GetOrCreateCounter(
				fmt.Sprintf(`vpcflow_exporter_pubsub_messages_ignored{project="%s",subscription="%s", reason="message_too_old"}`, projectID, subscriptionID),
			).Inc()
			return
		}

		labels := fmt.Sprintf(`reporter="%s",
			src_vpc_name="%s",
			src_vpc_subnetwork_name="%s",
			src_gke_cluster_location="%s",
			src_gke_cluster_name="%s",
			src_gke_pod_namespace="%s",
			src_gke_pod_name="%s",
			src_instance_project_id="%s",
			src_instance_region="%s",
			src_instance_vm_name="%s",
			src_instance_zone="%s"`,
			entry.JSONPayload.Reporter,
			entry.JSONPayload.SrcVPC.VPCName,
			entry.JSONPayload.SrcVPC.SubnetworkName,
			entry.JSONPayload.SrcGKE.Cluster.ClusterLocation,
			entry.JSONPayload.SrcGKE.Cluster.ClusterName,
			entry.JSONPayload.SrcGKE.Pod.PodNamespace,
			entry.JSONPayload.SrcGKE.Pod.PodName,
			entry.JSONPayload.SrcInstance.ProjectID,
			entry.JSONPayload.SrcInstance.Region,
			entry.JSONPayload.SrcInstance.VMName,
			entry.JSONPayload.SrcInstance.Zone,
		)

		if entry.JSONPayload.DestVPC.VPCName != "" {
			labels = fmt.Sprintf(`%s,
				dest_pvc_name="%s",
				dest_vpc_subnetwork_name="%s",
				dest_gke_cluster_location="%s",
				dest_gke_cluster_name="%s",
				dest_gke_pod_namespace="%s",
				dest_gke_pod_name="%s",
				dest_instance_project_id="%s",
				dest_instance_region="%s",
				dest_instance_vm_name="%s",
				dest_instance_zone="%s"`,
				labels,
				entry.JSONPayload.DestVPC.VPCName,
				entry.JSONPayload.DestVPC.SubnetworkName,
				entry.JSONPayload.DestGKE.Cluster.ClusterLocation,
				entry.JSONPayload.DestGKE.Cluster.ClusterName,
				entry.JSONPayload.DestGKE.Pod.PodNamespace,
				entry.JSONPayload.DestGKE.Pod.PodName,
				entry.JSONPayload.DestInstance.ProjectID,
				entry.JSONPayload.DestInstance.Region,
				entry.JSONPayload.DestInstance.VMName,
				entry.JSONPayload.DestInstance.Zone,
			)
		} else {
			// Ignored metrics due to destination missing per flow
			if os.Getenv("PROMETHEUS_VPC_FLOW_METRICS_ENABLE_MESSAGES_IGNORED_DESTINATION_MISSING_METRIC") == "true" {
				labels_formatted := strings.ReplaceAll(strings.ReplaceAll(labels, "\t", ""), "\n", "")
				metrics.GetOrCreateCounter(
					fmt.Sprintf(`vpcflow_exporter_pubsub_messages_ignored{project="%s",subscription="%s", reason="destination_missing", %s}`, projectID, subscriptionID, labels_formatted),
				).Inc()
			}

			// Ignored metrics due to destination missing, total
			metrics.GetOrCreateCounter(
				fmt.Sprintf(`vpcflow_exporter_pubsub_messages_ignored_total{project="%s",subscription="%s", reason="destination_missing"}`, projectID, subscriptionID),
			).Inc()
			return
		}

		labels_formatted := strings.ReplaceAll(strings.ReplaceAll(labels, "\t", ""), "\n", "")

		lastFlowReceivedTimesMutex.Lock()
		lastFlowReceivedTimes[labels_formatted] = time.Now()
		lastFlowReceivedTimesMutex.Unlock()

		bytes, _ := strconv.ParseInt(entry.JSONPayload.BytesSent, 10, 0)

		// Create counters with labels (labels are embedded in the metric name)
		metrics.GetOrCreateCounter(
			fmt.Sprintf(`vpcflow_bytes_total{%s}`, labels_formatted),
		).Add(int(bytes))

		if os.Getenv("PROMETHEUS_VPC_FLOW_METRICS_ENABLE_PACKET_METRICS") == "true" {
			packets, _ := strconv.ParseInt(entry.JSONPayload.PacketsSent, 10, 0)
			metrics.GetOrCreateCounter(
				fmt.Sprintf(`vpcflow_packets_total{%s}`, labels_formatted),
			).Add(int(packets))
		}

		metrics.GetOrCreateCounter(
			fmt.Sprintf(`vpcflow_exporter_pubsub_messages_processed{project="%s",subscription="%s"}`, projectID, subscriptionID),
		).Inc()

	})
	if err != nil {
		log.Fatalf("sub.Receive: %v", err)
	}
}

type LogEntry struct {
	InsertID         string     `json:"insertId"`
	JSONPayload      VPCFlowLog `json:"jsonPayload"`
	LogName          string     `json:"logName"`
	ReceiveTimestamp time.Time  `json:"receiveTimestamp"`
	Resource         Resource   `json:"resource"`
	Timestamp        time.Time  `json:"timestamp"`
}

type VPCFlowLog struct {
	BytesSent   string `json:"bytes_sent"`
	PacketsSent string `json:"packets_sent"`
	Reporter    string `json:"reporter"`
	RTTMillis   string `json:"rtt_msec,omitempty"`
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`

	Connection struct {
		SrcIP    string `json:"src_ip"`
		SrcPort  int    `json:"src_port"`
		DestIP   string `json:"dest_ip"`
		DestPort int    `json:"dest_port"`
		Protocol int    `json:"protocol"`
	} `json:"connection"`

	// Source-side metadata
	SrcGKE      GKEEndpoint      `json:"src_gke_details,omitempty"`
	SrcInstance InstanceEndpoint `json:"src_instance,omitempty"`
	SrcVPC      VPC              `json:"src_vpc,omitempty"`

	// Destination-side metadata
	DestGKE      GKEEndpoint      `json:"dest_gke_details,omitempty"`
	DestInstance InstanceEndpoint `json:"dest_instance,omitempty"`
	DestVPC      VPC              `json:"dest_vpc,omitempty"`
}

// --- Supporting types ---
type GKEEndpoint struct {
	Cluster struct {
		ClusterLocation string `json:"cluster_location"`
		ClusterName     string `json:"cluster_name"`
	} `json:"cluster"`
	Pod struct {
		PodName      string `json:"pod_name"`
		PodNamespace string `json:"pod_namespace"`
	} `json:"pod"`
}

type InstanceEndpoint struct {
	ManagedInstanceGroup struct {
		Name string `json:"name"`
		Zone string `json:"zone"`
	} `json:"managed_instance_group"`
	ProjectID string `json:"project_id"`
	Region    string `json:"region"`
	VMName    string `json:"vm_name"`
	Zone      string `json:"zone"`
}

type VPC struct {
	ProjectID        string `json:"project_id"`
	SubnetworkName   string `json:"subnetwork_name"`
	SubnetworkRegion string `json:"subnetwork_region"`
	VPCName          string `json:"vpc_name"`
}

type Resource struct {
	Type   string            `json:"type"`
	Labels map[string]string `json:"labels"`
}
