package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	podsv1alpha1 "k8s.io/kubelet/pkg/apis/pods/v1alpha1"
)

const (
	socketPath = "/var/lib/kubelet/pods-api/pods-api.sock"
)

var (
	interval = flag.Duration("interval", 0, "Interval for periodic List and Get checks (e.g. 5s, 1m). 0 means off.")
)

// watchResult holds the outcome from a single event from one of the watch streams.
type watchResult struct {
	watcherName string
	eventType   podsv1alpha1.EventType
	pod         *v1.Pod
	podBytes    []byte
	eventSize   int
	podSize     int
}

// watch is a function that runs a gRPC watch stream in a goroutine.
func watch(
	ctx context.Context,
	wg *sync.WaitGroup,
	watcherName string,
	client podsv1alpha1.PodsClient,
	results chan<- watchResult,
) {
	defer func() {
		wg.Done()
	}()
	log.Printf("[%s] Starting watch stream...", watcherName)

	reqCtx := ctx
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
		eventSize := proto.Size(event)
		podBytes := event.GetPod()
		podSize := len(podBytes)
		pod := &v1.Pod{}
		if podSize > 0 {
			if err := pod.Unmarshal(podBytes); err != nil {
				log.Printf("[%s] Failed to decode pod: %v", watcherName, err)
				continue
			}
		}

		results <- watchResult{
			watcherName: watcherName,
			eventType:   event.GetType(),
			pod:         pod,
			podBytes:    podBytes,
			eventSize:   eventSize,
			podSize:     podSize,
		}
	}
}

func main() {
	flag.Parse()
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

	var wg sync.WaitGroup
	results := make(chan watchResult)

	// Periodic List and Get checks
	if *interval > 0 {
		go func() {
			ticker := time.NewTicker(*interval)
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
	}

	wg.Add(1)
	// Launch the watch stream
	go watch(ctx, &wg, "Watcher   ", client, results)

	go func() {
		wg.Wait()
		close(results)
	}()

	log.Println("Watch stream started. Waiting for pod events...")

	type cachedPod struct {
		pod   *v1.Pod
		bytes []byte
	}
	podsCache := make(map[string]cachedPod)
	for result := range results {
		pod := result.pod
		uid := string(pod.UID)
		oldEntry, exists := podsCache[uid]
		oldPod := oldEntry.pod

		switch result.eventType {
		case podsv1alpha1.EventType_ADDED:
			log.Printf("EVENT ADDED    [%s]: %s/%s (EventSize: %d, PodSize: %d)", result.watcherName, pod.Namespace, pod.Name, result.eventSize, result.podSize)
			podsCache[uid] = cachedPod{pod: pod, bytes: result.podBytes}
		case podsv1alpha1.EventType_DELETED:
			log.Printf("EVENT DELETED  [%s]: %s/%s (EventSize: %d, PodSize: %d)", result.watcherName, pod.Namespace, pod.Name, result.eventSize, result.podSize)
			delete(podsCache, uid)
		case podsv1alpha1.EventType_MODIFIED:
			if exists {
				diff := cmp.Diff(oldPod, pod, cmpopts.IgnoreUnexported(v1.Pod{}, v1.PodSpec{}, v1.PodStatus{}, metav1.ObjectMeta{}))
				if diff != "" {
					log.Printf("EVENT MODIFIED [%s]: %s/%s (EventSize: %d, PodSize: %d)\nDiff:\n%s", result.watcherName, pod.Namespace, pod.Name, result.eventSize, result.podSize, diff)
				} else {
					if !bytes.Equal(oldEntry.bytes, result.podBytes) {
						log.Printf("EVENT MODIFIED [%s]: %s/%s (EventSize: %d, PodSize: %d, bytes changed but semantic diff is empty - likely unknown fields or internal metadata)", result.watcherName, pod.Namespace, pod.Name, result.eventSize, result.podSize)
					} else {
						log.Printf("EVENT MODIFIED [%s]: %s/%s (EventSize: %d, PodSize: %d, no changes in pod bytes)", result.watcherName, pod.Namespace, pod.Name, result.eventSize, result.podSize)
					}
				}
			} else {
				log.Printf("EVENT MODIFIED (new) [%s]: %s/%s (EventSize: %d, PodSize: %d)", result.watcherName, pod.Namespace, pod.Name, result.eventSize, result.podSize)
			}
			podsCache[uid] = cachedPod{pod: pod, bytes: result.podBytes}
		case podsv1alpha1.EventType_INITIAL_SYNC_COMPLETE:
			log.Printf("EVENT INITIAL_SYNC_COMPLETE [%s]: (EventSize: %d)", result.watcherName, result.eventSize)
		default:
			log.Printf("EVENT %-8s [%s]: %s/%s (EventSize: %d, PodSize: %d)", result.eventType, result.watcherName, pod.Namespace, pod.Name, result.eventSize, result.podSize)
			if uid != "" {
				podsCache[uid] = cachedPod{pod: pod, bytes: result.podBytes}
			}
		}
	}

	log.Println("--- Kubelet Pod Watcher Client Finished ---")
}
