# Release Process

If you are the current maintainer of this package:

1. Make sure your local version of rust is up-to-date: `rustup update`
1. Bump the version in `Cargo.toml`
1. Make sure dependencies are up-to-date: `cargo update`
1. Verify tests pass: `cargo test --target x86_64-unknown-linux-musl --release`
1. Verify builds succeed: `cargo build --target x86_64-unknown-linux-musl --release --locked`
1. Verify the minimal build succeed: `cargo build --target x86_64-unknown-linux-musl --release  --locked --no-default-features` 
1. Verify crate packaging works: `cargo package`
1. Test on multiple linux versions & distros (requires azure subscription): `./test/run.sh`
1. Tag the new version in git: `git tag vX.X.X`
1. Push the new version to GitHub: `git push --tags`
1. Create a [new release on GitHub](https://github.com/microsoft/avml/releases/new) with the aformentioned tag and populate it with this: `git log --pretty=format:"- %s" --reverse refs/tags/PREV_TAG...refs/tags/NEW_TAG`
1. Publish the crate: `cargo publish`
