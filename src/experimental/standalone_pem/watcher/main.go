package main

import (
    "bufio"
    "encoding/binary"
    "net"
    "os"
    "sync/atomic"
    "time"

    appsv1 "k8s.io/api/apps/v1"
    corev1 "k8s.io/api/core/v1"
    log "github.com/sirupsen/logrus"
    // metav1 is unused directly; rely on typed packages
    "k8s.io/client-go/informers"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/rest"
    "k8s.io/client-go/tools/cache"

    metadatapb "px.dev/pixie/src/shared/k8s/metadatapb"
)

// Simple UDS publisher that sends length-prefixed protobuf messages.
type Publisher struct {
    conn   net.Conn
    writer *bufio.Writer
}

func NewPublisher() (*Publisher, error) {
    uds := os.Getenv("PL_LOCAL_K8S_UPDATE_UDS")
    if uds == "" {
        uds = "/var/run/px-pem/mds.sock"
    }
    conn, err := net.DialTimeout("unix", uds, 5*time.Second)
    if err != nil {
        return nil, err
    }
    return &Publisher{conn: conn, writer: bufio.NewWriter(conn)}, nil
}

func (p *Publisher) Send(msg *metadatapb.ResourceUpdate) error {
    b, err := msg.Marshal()
    if err != nil {
        return err
    }
    var lenbuf [4]byte
    binary.BigEndian.PutUint32(lenbuf[:], uint32(len(b)))
    if _, err = p.writer.Write(lenbuf[:]); err != nil {
        return err
    }
    if _, err = p.writer.Write(b); err != nil {
        return err
    }
    return p.writer.Flush()
}

func (p *Publisher) Close() error { return p.conn.Close() }

var updateVersion int64 = time.Now().UnixNano()

func nextVersion() (cur int64, prev int64) {
    prev = atomic.LoadInt64(&updateVersion)
    cur = prev + 1
    atomic.StoreInt64(&updateVersion, cur)
    return cur, prev
}

func main() {
    // K8s client.
    cfg, err := rest.InClusterConfig()
    if err != nil {
        log.Fatalf("in-cluster config: %v", err)
    }
    cs, err := kubernetes.NewForConfig(cfg)
    if err != nil {
        log.Fatalf("clientset: %v", err)
    }

    pub, err := NewPublisher()
    if err != nil {
        log.Fatalf("publisher: %v", err)
    }
    defer pub.Close()

    // Informers.
    factory := informers.NewSharedInformerFactory(cs, 0)

    // Namespaces.
    nsInf := factory.Core().V1().Namespaces().Informer()
    nsInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            ns := obj.(*corev1.Namespace)
            cur, prev := nextVersion()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            upd.Update = &metadatapb.ResourceUpdate_NamespaceUpdate{NamespaceUpdate: &metadatapb.NamespaceUpdate{
                UID:               string(ns.UID),
                Name:              ns.Name,
                StartTimestampNS:  ns.CreationTimestamp.UnixNano(),
            }}
            if err := pub.Send(upd); err != nil { log.Warnf("send ns add: %v", err) }
        },
        UpdateFunc: func(_, obj interface{}) {
            ns := obj.(*corev1.Namespace)
            cur, prev := nextVersion()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            upd.Update = &metadatapb.ResourceUpdate_NamespaceUpdate{NamespaceUpdate: &metadatapb.NamespaceUpdate{
                UID:               string(ns.UID),
                Name:              ns.Name,
                StartTimestampNS:  ns.CreationTimestamp.UnixNano(),
            }}
            if err := pub.Send(upd); err != nil { log.Warnf("send ns upd: %v", err) }
        },
        DeleteFunc: func(obj interface{}) {
            ns := obj.(*corev1.Namespace)
            cur, prev := nextVersion()
            now := time.Now().UnixNano()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            upd.Update = &metadatapb.ResourceUpdate_NamespaceUpdate{NamespaceUpdate: &metadatapb.NamespaceUpdate{
                UID:              string(ns.UID),
                Name:             ns.Name,
                StopTimestampNS:  now,
            }}
            if err := pub.Send(upd); err != nil { log.Warnf("send ns del: %v", err) }
        },
    })

    // Nodes.
    nodeInf := factory.Core().V1().Nodes().Informer()
    nodeInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            n := obj.(*corev1.Node)
            cur, prev := nextVersion()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            upd.Update = &metadatapb.ResourceUpdate_NodeUpdate{NodeUpdate: &metadatapb.NodeUpdate{
                UID:               string(n.UID),
                Name:              n.Name,
                StartTimestampNS:  n.CreationTimestamp.UnixNano(),
            }}
            if err := pub.Send(upd); err != nil { log.Warnf("send node add: %v", err) }
        },
        UpdateFunc: func(_, obj interface{}) {
            n := obj.(*corev1.Node)
            cur, prev := nextVersion()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            upd.Update = &metadatapb.ResourceUpdate_NodeUpdate{NodeUpdate: &metadatapb.NodeUpdate{
                UID:               string(n.UID),
                Name:              n.Name,
                StartTimestampNS:  n.CreationTimestamp.UnixNano(),
            }}
            if err := pub.Send(upd); err != nil { log.Warnf("send node upd: %v", err) }
        },
        DeleteFunc: func(obj interface{}) {
            n := obj.(*corev1.Node)
            cur, prev := nextVersion()
            now := time.Now().UnixNano()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            upd.Update = &metadatapb.ResourceUpdate_NodeUpdate{NodeUpdate: &metadatapb.NodeUpdate{
                UID:              string(n.UID),
                Name:             n.Name,
                StopTimestampNS:  now,
            }}
            if err := pub.Send(upd); err != nil { log.Warnf("send node del: %v", err) }
        },
    })

    // Services.
    svcInf := factory.Core().V1().Services().Informer()
    svcInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            s := obj.(*corev1.Service)
            cur, prev := nextVersion()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            su := &metadatapb.ServiceUpdate{
                UID:               string(s.UID),
                Name:              s.Name,
                Namespace:         s.Namespace,
                StartTimestampNS:  s.CreationTimestamp.UnixNano(),
            }
            if s.Spec.ClusterIP != "" {
                su.ClusterIP = s.Spec.ClusterIP
            }
            upd.Update = &metadatapb.ResourceUpdate_ServiceUpdate{ServiceUpdate: su}
            if err := pub.Send(upd); err != nil { log.Warnf("send svc add: %v", err) }
        },
        UpdateFunc: func(_, obj interface{}) {
            s := obj.(*corev1.Service)
            cur, prev := nextVersion()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            su := &metadatapb.ServiceUpdate{
                UID:               string(s.UID),
                Name:              s.Name,
                Namespace:         s.Namespace,
                StartTimestampNS:  s.CreationTimestamp.UnixNano(),
            }
            if s.Spec.ClusterIP != "" {
                su.ClusterIP = s.Spec.ClusterIP
            }
            upd.Update = &metadatapb.ResourceUpdate_ServiceUpdate{ServiceUpdate: su}
            if err := pub.Send(upd); err != nil { log.Warnf("send svc upd: %v", err) }
        },
        DeleteFunc: func(obj interface{}) {
            s := obj.(*corev1.Service)
            cur, prev := nextVersion()
            now := time.Now().UnixNano()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            upd.Update = &metadatapb.ResourceUpdate_ServiceUpdate{ServiceUpdate: &metadatapb.ServiceUpdate{
                UID:              string(s.UID),
                Name:             s.Name,
                Namespace:        s.Namespace,
                StopTimestampNS:  now,
            }}
            if err := pub.Send(upd); err != nil { log.Warnf("send svc del: %v", err) }
        },
    })

    // Pods + Containers.
    podInf := factory.Core().V1().Pods().Informer()
    podInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            p := obj.(*corev1.Pod)
            cur, prev := nextVersion()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            pu := &metadatapb.PodUpdate{
                UID:               string(p.UID),
                Name:              p.Name,
                Namespace:         p.Namespace,
                NodeName:          p.Spec.NodeName,
                StartTimestampNS:  p.CreationTimestamp.UnixNano(),
            }
            // containers
            for _, cs := range p.Status.ContainerStatuses {
                cid := cs.ContainerID
                // ContainerID format may be like "containerd://<id>"; keep raw for now.
                cu := &metadatapb.ContainerUpdate{
                    CID:               cid,
                    Name:              cs.Name,
                    Namespace:         p.Namespace,
                    PodID:             string(p.UID),
                    PodName:           p.Name,
                    StartTimestampNS:  p.CreationTimestamp.UnixNano(),
                }
                cuUpd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev,
                    Update: &metadatapb.ResourceUpdate_ContainerUpdate{ContainerUpdate: cu}}
                if err := pub.Send(cuUpd); err != nil { log.Warnf("send container add: %v", err) }
            }
            upd.Update = &metadatapb.ResourceUpdate_PodUpdate{PodUpdate: pu}
            if err := pub.Send(upd); err != nil { log.Warnf("send pod add: %v", err) }
        },
        UpdateFunc: func(_, obj interface{}) {
            p := obj.(*corev1.Pod)
            cur, prev := nextVersion()
            pu := &metadatapb.PodUpdate{
                UID:               string(p.UID),
                Name:              p.Name,
                Namespace:         p.Namespace,
                NodeName:          p.Spec.NodeName,
                StartTimestampNS:  p.CreationTimestamp.UnixNano(),
            }
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev,
                Update: &metadatapb.ResourceUpdate_PodUpdate{PodUpdate: pu}}
            if err := pub.Send(upd); err != nil { log.Warnf("send pod upd: %v", err) }
        },
        DeleteFunc: func(obj interface{}) {
            p := obj.(*corev1.Pod)
            cur, prev := nextVersion()
            now := time.Now().UnixNano()
            pu := &metadatapb.PodUpdate{
                UID:              string(p.UID),
                Name:             p.Name,
                Namespace:        p.Namespace,
                StopTimestampNS:  now,
            }
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev,
                Update: &metadatapb.ResourceUpdate_PodUpdate{PodUpdate: pu}}
            if err := pub.Send(upd); err != nil { log.Warnf("send pod del: %v", err) }
        },
    })

    // ReplicaSets.
    rsInf := factory.Apps().V1().ReplicaSets().Informer()
    rsInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            rs := obj.(*appsv1.ReplicaSet)
            cur, prev := nextVersion()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            upd.Update = &metadatapb.ResourceUpdate_ReplicaSetUpdate{ReplicaSetUpdate: &metadatapb.ReplicaSetUpdate{
                UID:               string(rs.UID),
                Name:              rs.Name,
                Namespace:         rs.Namespace,
                StartTimestampNS:  rs.CreationTimestamp.UnixNano(),
            }}
            if err := pub.Send(upd); err != nil { log.Warnf("send rs add: %v", err) }
        },
        UpdateFunc: func(_, obj interface{}) {
            rs := obj.(*appsv1.ReplicaSet)
            cur, prev := nextVersion()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            upd.Update = &metadatapb.ResourceUpdate_ReplicaSetUpdate{ReplicaSetUpdate: &metadatapb.ReplicaSetUpdate{
                UID:               string(rs.UID),
                Name:              rs.Name,
                Namespace:         rs.Namespace,
                StartTimestampNS:  rs.CreationTimestamp.UnixNano(),
            }}
            if err := pub.Send(upd); err != nil { log.Warnf("send rs upd: %v", err) }
        },
        DeleteFunc: func(obj interface{}) {
            rs := obj.(*appsv1.ReplicaSet)
            cur, prev := nextVersion()
            now := time.Now().UnixNano()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            upd.Update = &metadatapb.ResourceUpdate_ReplicaSetUpdate{ReplicaSetUpdate: &metadatapb.ReplicaSetUpdate{
                UID:              string(rs.UID),
                Name:             rs.Name,
                Namespace:        rs.Namespace,
                StopTimestampNS:  now,
            }}
            if err := pub.Send(upd); err != nil { log.Warnf("send rs del: %v", err) }
        },
    })

    // Deployments.
    depInf := factory.Apps().V1().Deployments().Informer()
    depInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            d := obj.(*appsv1.Deployment)
            cur, prev := nextVersion()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            upd.Update = &metadatapb.ResourceUpdate_DeploymentUpdate{DeploymentUpdate: &metadatapb.DeploymentUpdate{
                UID:               string(d.UID),
                Name:              d.Name,
                Namespace:         d.Namespace,
                StartTimestampNS:  d.CreationTimestamp.UnixNano(),
            }}
            if err := pub.Send(upd); err != nil { log.Warnf("send dep add: %v", err) }
        },
        UpdateFunc: func(_, obj interface{}) {
            d := obj.(*appsv1.Deployment)
            cur, prev := nextVersion()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            upd.Update = &metadatapb.ResourceUpdate_DeploymentUpdate{DeploymentUpdate: &metadatapb.DeploymentUpdate{
                UID:               string(d.UID),
                Name:              d.Name,
                Namespace:         d.Namespace,
                StartTimestampNS:  d.CreationTimestamp.UnixNano(),
            }}
            if err := pub.Send(upd); err != nil { log.Warnf("send dep upd: %v", err) }
        },
        DeleteFunc: func(obj interface{}) {
            d := obj.(*appsv1.Deployment)
            cur, prev := nextVersion()
            now := time.Now().UnixNano()
            upd := &metadatapb.ResourceUpdate{UpdateVersion: cur, PrevUpdateVersion: prev}
            upd.Update = &metadatapb.ResourceUpdate_DeploymentUpdate{DeploymentUpdate: &metadatapb.DeploymentUpdate{
                UID:              string(d.UID),
                Name:             d.Name,
                Namespace:        d.Namespace,
                StopTimestampNS:  now,
            }}
            if err := pub.Send(upd); err != nil { log.Warnf("send dep del: %v", err) }
        },
    })

    stop := make(chan struct{})
    factory.Start(stop)
    for t, ready := range factory.WaitForCacheSync(stop) {
        if !ready {
            log.Warnf("informer %v not synced", t)
        }
    }
    <-stop
}
