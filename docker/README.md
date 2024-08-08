This Dockerfile is only used to build the image for KubeBlocks. It only contains the necessary OpenTenBase binaries, but no entrypoint or command to run the OpenTenBase cluster.

To build and push the image, run the following command in the root directory of the repository:

```bash
make push-image
```

NOTE:
- The image is pushed to the `damainlau/opentenbase` repository, should change it before merge to the main branch.