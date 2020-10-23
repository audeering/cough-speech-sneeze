# ravdess

Converts the [cough-speech-sneeze-raw-data] data set to the [Unified Format] using
[audata], and publishes it again on Artifactory.

## Prerequisites

[Java] must be installed.

## Publishing

Run `./gradlew publish`.

An [Artifactory] account with deploy permissions must be configured via Gradle properties `artifactoryUser` and `artifactoryApiKey`.


[cough-speech-sneeze-raw-data]: https://gitlab.audeering.com/data/cough-speech-sneeze-raw-data
[Unified Format]: http://tools.pp.audeering.com/audata/data-format.html
[audata]: https://gitlab.audeering.com/tools/pyaudata
[Artifactory]: http://artifactory.audeering.local/
[Java]: https://sdkman.io/sdks#java
