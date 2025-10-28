package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	podsv1alpha1 "k8s.io/kubelet/pkg/apis/pods/v1alpha1"
)

const (
	socketPath           = "/var/lib/kubelet/pods-api/pods-api.sock"
	fieldMaskMetadataKey = "x-kubernetes-fieldmask"
)

// watchResult holds the outcome from a single event from one of the watch streams.
type watchResult struct {
	watcherName string
	eventType   podsv1alpha1.EventType
	pod         *v1.Pod
	messageSize int
}

// watch is a function that runs a gRPC watch stream in a goroutine.
func watch(
	ctx context.Context,
	wg *sync.WaitGroup,
	watcherName string,
	client podsv1alpha1.PodsClient,
	maskPaths []string,
	results chan<- watchResult,
) {
	defer func() {
		wg.Done()
	}()
	log.Printf("[%s] Starting watch stream...", watcherName)

	reqCtx := ctx
	if len(maskPaths) > 0 {
		headerValue := strings.Join(maskPaths, ",")
		log.Printf("[%s] Attaching metadata: '%s: %s'", watcherName, fieldMaskMetadataKey, headerValue)

		md := metadata.New(map[string]string{fieldMaskMetadataKey: headerValue})
		reqCtx = metadata.NewOutgoingContext(ctx, md)
	}

	req := &podsv1alpha1.WatchPodsRequest{}

	stream, err := client.WatchPods(reqCtx, req)
	if err != nil {
		log.Printf("[%s] Failed to start pod watch: %v", watcherName, err)
		return
	}
	log.Printf("[%s] Watch stream started successfully.", watcherName)

	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Printf("[%s] Server closed the stream (EOF).", watcherName)
				return
			}
			select {
			case <-ctx.Done():
				log.Printf("[%s] Watch stream context cancelled.", watcherName)
			default:
				log.Printf("[%s] Error receiving from stream: %v", watcherName, err)
			}
			return
		}

		if event == nil {
			log.Printf("[%s] Received nil event from stream. Skipping.", watcherName)
			continue
		}
		size := proto.Size(event)
		pod := &v1.Pod{}
		if err := pod.Unmarshal(event.GetPod()); err != nil {
			log.Printf("[%s] Failed to decode pod: %v", watcherName, err)
			continue
		}

		results <- watchResult{
			watcherName: watcherName,
			eventType:   event.GetType(),
			pod:         pod,
			messageSize: size,
		}
	}
}

func main() {
	log.Println("--- Kubelet Pod Watcher Client ---")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		return net.Dial("unix", socketPath)
	}

	conn, err := grpc.DialContext(ctx,
		socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	if err != nil {
		log.Fatalf("Failed to dial gRPC server: %v", err)
	}
	defer conn.Close()

	log.Printf("Successfully connected to %s", socketPath)
	client := podsv1alpha1.NewPodsClient(conn)

	fieldMaskPaths := []string{
		"metadata.name",
		"metadata.namespace",
		"spec.containers.name",
		"spec.initContainers.name",
		"spec.initContainers.restartPolicy",
		"spec.ephemeralContainers.name",
		"status.phase",
		"status.containerStatuses.name",
		"status.containerStatuses.ready",
		"status.initContainerStatuses.name",
		"status.initContainerStatuses.ready",
		"status.ephemeralContainerStatuses.name",
		"status.ephemeralContainerStatuses.ready",
	}

	var wg sync.WaitGroup
	results := make(chan watchResult)

	// Periodic List and Get checks
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Println("--- Periodic List Check ---")
				listResp, err := client.ListPods(ctx, &podsv1alpha1.ListPodsRequest{})
				if err != nil {
					log.Printf("ListPods failed: %v", err)
					continue
				}

				podsList := listResp.GetPods()
				log.Printf("ListPods returned %d pods", len(podsList))

				if len(podsList) > 0 {
					p := &v1.Pod{}
					// Assuming the list returns items compatible with Unmarshal (likely []byte)
					if err := p.Unmarshal(podsList[0]); err != nil {
						log.Printf("Failed to unmarshal first pod from list: %v", err)
						continue
					}

					log.Printf("Attempting GetPod for %s/%s (UID: %s)", p.Namespace, p.Name, p.UID)
					getResp, err := client.GetPod(ctx, &podsv1alpha1.GetPodRequest{
						PodUID: string(p.UID),
					})
					if err != nil {
						log.Printf("GetPod failed: %v", err)
						continue
					}

					p2 := &v1.Pod{}
					if err := p2.Unmarshal(getResp.GetPod()); err != nil {
						log.Printf("Failed to unmarshal pod from GetPod: %v", err)
						continue
					}
					b, _ := json.MarshalIndent(p2, "", "  ")
					log.Printf("Successfully performed GetPod for %s/%s:\n%s", p2.Namespace, p2.Name, string(b))
				}
			}
		}
	}()

	wg.Add(2)
	// Launch the unmasked watch (sends no metadata)
	go watch(ctx, &wg, "Unmasked  ", client, nil, results)

	// Launch the masked watch (sends the 'x-kubernetes-fieldmask' metadata)
	go watch(ctx, &wg, "Masked    ", client, fieldMaskPaths, results)

	go func() {
		wg.Wait()
		close(results)
	}()

	log.Println("Both watch streams started. Waiting for pod events...")

	for result := range results {
		b, err := json.MarshalIndent(result.pod, "", "  ")
		if err != nil {
			log.Printf("Failed to marshal pod: %v", err)
			continue
		}
		prettyPod := string(b)
		log.Printf("EVENT %s [%s]: Size: %4d bytes\n---\n%s\n---",
			result.eventType,
			result.watcherName,
			result.messageSize,
			prettyPod,
		)
	}

	log.Println("--- Kubelet Pod Watcher Client Finished ---")
}
