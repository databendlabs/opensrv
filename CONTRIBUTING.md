# Contributing

This is a Rust project, so [rustup](https://rustup.rs/) is the best place to start.

This is a pure rust project, so only `cargo` is needed.

- `cargo check` to analyze the current package and report errors.
- `cargo build` to compile the current package.
- `cargo clippy` to catch common mistakes and improve code.
- `cargo test` to run unit tests.

Useful tips:

- Check/Build/Test/Clippy all code: `cargo <cmd> --workspace`
- Check/Build/Test/Clippy specific package: `cargo <cmd> --package <package>`
- Test specific function: `cargo test --package opensrv-mysql r#async::it_connects`

For changelog:

```
pip install git-changelog
git-changelog -o CHANGELOG.md -c angular -t keepachangelog .
```
