# LOINC Pipeline

**Prerequisites**

* JDK 24+
* Maven 3.9.9+
* Nexus Repository (optional)

**Clone Project and Configure Maven Settings**

1. Clone the [loinc-data repository](https://github.com/ikmdev/loinc-data)

   ```
   git clone https://github.com/ikmdev/loinc-data.git
   ```

2. Configure Maven settings.xml based on the [provided sample](https://ikmdev.atlassian.net/wiki/spaces/IKDT/pages/1036648449/Centralized+Documentation+for+Maven+Settings+File+Configuration).

3. Change local directory to `loinc-data`

**Run Origin Packaging**

The following source data is required for this pipeline and can be obtained from LOINC:

* Loinc_2.81.zip

More information can be found on: https://loinc.org/downloads/

1. Place the downloaded ZIPs in your ~/Downloads directory.

2. Ensure the properties defined in loinc-data/pom.xml are set to the correct file names:
   - <source.zip>

3. Run origin packaging and deployment.

   To deploy origin artifact to a shared Nexus repository, run the following command, specifying the repository ID and URL in `-DaltDeploymentRepository`
   ```
   mvn --projects loinc-origin --also-make clean deploy -Ptinkarbuild -DaltDeploymentRepository=tinkar-snapshot::https://nexus.tinkar.org/repository/maven-snapshots/ -Dmaven.build.cache.enabled=false
   ```

   To install origin artifact to a local M2 repository, run the following command:
   ```
   mvn --projects loinc-origin --also-make clean install -Ptinkarbuild,generateDataLocal -Dmaven.build.cache.enabled=false
   ```

**Run Transformation Pipeline**

The transformation pipeline can be built after origin data is available in Nexus or a local M2 repository.

1. Build the pipeline with the following command:
   ```
   mvn clean install -U -Ptinkarbuild -Dmaven.build.cache.enabled=false
   ```

2. Deploy transformed data artifacts to Nexus, run the following command:
   ```
   mvn --projects loinc-export --also-make deploy -Ptinkarbuild -DaltDeploymentRepository=tinkar-snapshot::https://nexus.tinkar.org/repository/maven-snapshots/ -Dmaven.build.cache.enabled=false
   ```
   
