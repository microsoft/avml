# Release Process

If you are the current maintainer of this package:

1. Bump the version in `Cargo.toml`
1. Build & Test on multiple linux versions & distros: `./eng/release.sh`
1. Tag the new version in git: `git tag vX.X.X`
1. Push the new version to GitHub: `git push --tags`
1. Create a [new release on GitHub](https://github.com/microsoft/avml/releases/new) with the aforementioned tag and populate it with this: `git log --pretty=format:"- %s" --reverse refs/tags/PREV_TAG...refs/tags/NEW_TAG`
1. Publish the crate: `cargo publish`

*NOTE*: You must be logged in to an azure subscription via `az login`.
