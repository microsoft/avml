# Release Process

The following procedures assume the following:
1. You are running on an Ubuntu based system.  (Currently tested using Ubuntu 20.04)
1. You can already successfully build and test AVML using `./eng/ci.sh`
1. You are logged into an Azure subscription using `az login`.
1. You are logged into [crates.io](https://crates.io) using `cargo login`
1. You install `sudo` to root

If you are the current maintainer of this package:

1. Create a branch for updating the version number of AVML
1. Bump the version in `Cargo.toml`
1. Build & Locally test with the updated version using: `./eng/ci.sh`
1. Test on multiple linux versions using: `./eng/test-on-azure.sh`
1. Commit the updated `Cargo.toml` and `Cargo.lock`
1. Submit & merge a PR from this branch with the updated version information to the [git repo](https://github.com/microsoft/avml).
1. After the PR is merged, pull down and checkout `main`.
1. Verify the source as is can be packaged for crates.io using: `cargo package --locked`
1. Tag the new version in git: `git tag vX.X.X`
1. Push the new version to GitHub: `git push --tags`
1. Create a [new release on GitHub](https://github.com/microsoft/avml/releases/new) with the aforementioned tag and populate it with this: `git log --pretty=format:"- %s" --reverse refs/tags/PREV_TAG...refs/tags/NEW_TAG`
1. Add the build artifacts from [GitHub Actions](https://github.com/microsoft/avml/actions)
1. Publish the crate: `cargo publish`
