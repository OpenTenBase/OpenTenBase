# OpenTenBase Add-on for KubeBlocks

This folder contains two Helm charts for deploying the OpenTenBase KubeBlocks add-on on a Kubernetes cluster. Use this add-on, you can deploy and manage OpenTenBase clusters on Kubernetes.

## What is KubeBlocks?

[KubeBlocks](https://github.com/apecloud/kubeblocks) is an open-source control plane software that runs and manages databases, message queues and other data infrastructure on K8s. The name KubeBlocks is inspired by Kubernetes and LEGO blocks, signifying that running and managing data infrastructure on K8s can be standard and productive, like playing with LEGO blocks.

More information about KubeBlocks can be found in the [KubeBlocks GitHub repository](https://github.com/apecloud/kubeblocks) and the [KubeBlocks website](https://kubeblocks.io/). You can find all supported add-ons in the [KubeBlocks Add-ons GitHub repository](https://github.com/apecloud/kubeblocks-addons).

## Prerequisites

Before you deploy the OpenTenBase KubeBlocks add-on, you need to have the following prerequisites:
* A Kubernetes cluster, you can use [minikube](https://minikube.sigs.k8s.io/docs/) to create a local cluster for testing
* The `kubectl` command-line tool installed on your local machine
* The `helm` command-line tool installed on your local machine
* The `kbcli` command-line tool to install the KubeBlocks control plane

## Install KubeBlocks

Refer to the [KubeBlocks installation guide](https://kubeblocks.io/docs/release-0.8/user_docs/installation/install-with-kbcli/install-kbcli) to install the `kbcli` and  KubeBlocks control plane on your Kubernetes cluster.

## Deploy OpenTenBase KubeBlocks Add-on

To deploy the OpenTenBase KubeBlocks add-on on your Kubernetes cluster, follow these steps:

```bash
helm install opentenbase ./opentenbase
```

Check the ClusterDefition and ClusterVersion:

```bash



