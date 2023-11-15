# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

<!-- insertion marker -->
## [v0.5.0](https://github.com/PsiACE/opensrv/releases/tag/v0.5.0) - 2023-11-06

<small>[Compare with latest](https://github.com/PsiACE/opensrv/compare/v0.4.1...v0.5.0)</small>

### Features

- try to release v0.5.0 ([df0cd64](https://github.com/PsiACE/opensrv/commit/df0cd64bdb49ac57e7afee83e4f9c86161020f09) by Chojan Shang).

<!-- insertion marker -->
## [v0.4.1](https://github.com/PsiACE/opensrv/releases/tag/v0.4.1) - 2023-09-09

<small>[Compare with v0.4.0](https://github.com/PsiACE/opensrv/compare/v0.4.0...v0.4.1)</small>

### Bug Fixes

- try to fix memory safety problem ([2f84755](https://github.com/PsiACE/opensrv/commit/2f84755dfa0ec32de752b7530580f985cb027896) by Chojan Shang).

## [v0.4.0](https://github.com/PsiACE/opensrv/releases/tag/v0.4.0) - 2023-04-11

<small>[Compare with v0.3.0](https://github.com/PsiACE/opensrv/compare/v0.3.0...v0.4.0)</small>

### Features

- add marashal/unmarshal support for large size integer types. (#45) ([a97d750](https://github.com/PsiACE/opensrv/commit/a97d75058baf4fc20031b1c9668ff94e7e4e542e) by RinChanNOW).
- make version() return &str to String ([a6d29b1](https://github.com/PsiACE/opensrv/commit/a6d29b1cd3c6b43f6c0eef10fd2e9ad30635ec51) by arthur-zhang).
- add option to reject connection when dbname absence in login (#38) ([b44c9d1](https://github.com/PsiACE/opensrv/commit/b44c9d1360da297b305abf33aecfa94888e1554c) by Ning Sun).
- packet reader reduce bytes resize times ([32af58d](https://github.com/PsiACE/opensrv/commit/32af58dd9fd9be66c39dd0728142d78e831f6fb6) by baishen).

### Bug Fixes

- corrupt tls handshake caused by buffer over read (#39) ([4f6400c](https://github.com/PsiACE/opensrv/commit/4f6400cab379bce3b0b35b6753e7cdc6a8d50a8b) by Ning Sun).
- make clippy happy (#41) ([564e62e](https://github.com/PsiACE/opensrv/commit/564e62e34cd4b06a7c75a47cac271c17637401b0) by Chojan Shang).

## [v0.3.0](https://github.com/PsiACE/opensrv/releases/tag/v0.3.0) - 2022-11-26

<small>[Compare with v0.2.0](https://github.com/PsiACE/opensrv/compare/v0.2.0...v0.3.0)</small>

### Features

- add tls support for opensrv-mysql (#34) ([3a984ec](https://github.com/PsiACE/opensrv/commit/3a984ec1b4046d9b2b8da58abfe5d8921715ddeb) by SSebo).
- bump main deps (#33) ([1b3e11d](https://github.com/PsiACE/opensrv/commit/1b3e11d73bd5f0fcad1401df1620b3bbb5b7a0f6) by Chojan Shang).
- bump version to 0.2.1 ([0f488d0](https://github.com/PsiACE/opensrv/commit/0f488d0041f4979f88ace93b3ce41b72713f93f0) by sundyli).
- remove unused clippy ([161b5a9](https://github.com/PsiACE/opensrv/commit/161b5a97e435aefecd2877c894e20e422aa39de9) by sundyli).
- add orderfloat ([c875ddd](https://github.com/PsiACE/opensrv/commit/c875ddd29051c3a62462a96caba3eb9792335149) by sundyli).

## [v0.2.0](https://github.com/PsiACE/opensrv/releases/tag/v0.2.0) - 2022-08-17

<small>[Compare with v0.1.0](https://github.com/PsiACE/opensrv/compare/v0.1.0...v0.2.0)</small>

### Features

- Implement proposal Simplify ClickHouseSession (#26) ([a757e28](https://github.com/PsiACE/opensrv/commit/a757e286f49ca3653ff3b972615842fb34f98297) by Xuanwo).

### Code Refactoring

- write mysql resultset in a streaming way (#27) ([1287c32](https://github.com/PsiACE/opensrv/commit/1287c32cec4242fa2a440e1a9b7ffeab63ea76a8) by dantengsky).

## [v0.1.0](https://github.com/PsiACE/opensrv/releases/tag/v0.1.0) - 2022-06-14

<small>[Compare with first commit](https://github.com/PsiACE/opensrv/compare/eff4ec6872504b271b93b1c61a223f9386a29e47...v0.1.0)</small>

### Features

- add a new marshal mod from databend's common-io (#20) ([d29655a](https://github.com/PsiACE/opensrv/commit/d29655a73ed26d94733861de83fa764e29ad2f78) by Chojan Shang).
- datafuse-extras/msql-srv -> opensrv-mysql (#3) ([86d1be8](https://github.com/PsiACE/opensrv/commit/86d1be8bf56dcc5be18d49340041b4c57de3a29f) by Chojan Shang).
- common/clickhouse-srv -> opensrv-clickhouse (#1) ([183b728](https://github.com/PsiACE/opensrv/commit/183b7281ca014033d70616ecab1046df5000fa9c) by Chojan Shang).

### Bug Fixes

- spawn to fix sync tests (#17) ([54638ee](https://github.com/PsiACE/opensrv/commit/54638ee8b5abb12aa8c0f63469ce78a900223c0a) by Chojan Shang).
- pass federated query ([967477f](https://github.com/PsiACE/opensrv/commit/967477f1f7005f8911a7d6c38cefbba4edd755ad) by zhang2014).
- add init schema handle on handshake (#11) ([e744427](https://github.com/PsiACE/opensrv/commit/e744427ebce9271289e47d655f1223790aae7482) by Jun).
- make authenticate async to fix hang issue (#10) ([9690be9](https://github.com/PsiACE/opensrv/commit/9690be9ff965c0e86e1ff599897c2019b0a379bd) by Chojan Shang).

### Code Refactoring

- make auth_plugin_for_username async (#15) ([4e447f8](https://github.com/PsiACE/opensrv/commit/4e447f8e64619b78c84c2c10f87574b1ae64a5ca) by Yang Xiufeng).

