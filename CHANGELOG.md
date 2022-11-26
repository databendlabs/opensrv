# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [v0.3.0](https://github.com/datafuselabs/opensrv/releases/tag/v0.3.0) - 2022-11-26

<small>[Compare with v0.2.0](https://github.com/datafuselabs/opensrv/compare/v0.2.0...v0.3.0)</small>

### Features
- Add tls support for opensrv-mysql (#34) ([3a984ec](https://github.com/datafuselabs/opensrv/commit/3a984ec1b4046d9b2b8da58abfe5d8921715ddeb) by SSebo).
- Bump main deps (#33) ([1b3e11d](https://github.com/datafuselabs/opensrv/commit/1b3e11d73bd5f0fcad1401df1620b3bbb5b7a0f6) by Chojan Shang).
- Bump version to 0.2.1 ([0f488d0](https://github.com/datafuselabs/opensrv/commit/0f488d0041f4979f88ace93b3ce41b72713f93f0) by sundyli).
- Remove unused clippy ([161b5a9](https://github.com/datafuselabs/opensrv/commit/161b5a97e435aefecd2877c894e20e422aa39de9) by sundyli).
- Add orderfloat ([c875ddd](https://github.com/datafuselabs/opensrv/commit/c875ddd29051c3a62462a96caba3eb9792335149) by sundyli).


## [v0.2.0](https://github.com/datafuselabs/opensrv/releases/tag/v0.2.0) - 2022-08-17

<small>[Compare with v0.1.0](https://github.com/datafuselabs/opensrv/compare/v0.1.0...v0.2.0)</small>

### Code Refactoring
- Write mysql resultset in a streaming way (#27) ([1287c32](https://github.com/datafuselabs/opensrv/commit/1287c32cec4242fa2a440e1a9b7ffeab63ea76a8) by dantengsky).

### Features
- Implement proposal simplify clickhousesession (#26) ([a757e28](https://github.com/datafuselabs/opensrv/commit/a757e286f49ca3653ff3b972615842fb34f98297) by Xuanwo).


## [v0.1.0](https://github.com/datafuselabs/opensrv/releases/tag/v0.1.0) - 2022-06-14

<small>[Compare with first commit](https://github.com/datafuselabs/opensrv/compare/eff4ec6872504b271b93b1c61a223f9386a29e47...v0.1.0)</small>

### Bug Fixes
- Spawn to fix sync tests (#17) ([54638ee](https://github.com/datafuselabs/opensrv/commit/54638ee8b5abb12aa8c0f63469ce78a900223c0a) by Chojan Shang).
- Pass federated query ([967477f](https://github.com/datafuselabs/opensrv/commit/967477f1f7005f8911a7d6c38cefbba4edd755ad) by zhang2014).
- Add init schema handle on handshake (#11) ([e744427](https://github.com/datafuselabs/opensrv/commit/e744427ebce9271289e47d655f1223790aae7482) by Jun).
- Make authenticate async to fix hang issue (#10) ([9690be9](https://github.com/datafuselabs/opensrv/commit/9690be9ff965c0e86e1ff599897c2019b0a379bd) by Chojan Shang).

### Code Refactoring
- Make auth_plugin_for_username async (#15) ([4e447f8](https://github.com/datafuselabs/opensrv/commit/4e447f8e64619b78c84c2c10f87574b1ae64a5ca) by Yang Xiufeng).

### Features
- Add a new marshal mod from databend's common-io (#20) ([d29655a](https://github.com/datafuselabs/opensrv/commit/d29655a73ed26d94733861de83fa764e29ad2f78) by Chojan Shang).
- Datafuse-extras/msql-srv -> opensrv-mysql (#3) ([86d1be8](https://github.com/datafuselabs/opensrv/commit/86d1be8bf56dcc5be18d49340041b4c57de3a29f) by Chojan Shang).
- Common/clickhouse-srv -> opensrv-clickhouse (#1) ([183b728](https://github.com/datafuselabs/opensrv/commit/183b7281ca014033d70616ecab1046df5000fa9c) by Chojan Shang).


