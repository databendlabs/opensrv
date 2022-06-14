# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

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


