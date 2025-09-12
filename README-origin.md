# loinc-data

### Team Ownership - Product Owner
Data Team

## Getting Started

Follow these instructions to generate a loinc data ORIGIN dataset:

1. Clone the [loinc-data repository](https://github.com/ikmdev/loinc-data)

```bash
git clone [Rep URL]
```

2. Change local directory to `loinc-data\loinc-origin`

3. Ensure the loinc-data/pom.xml contains the proper tags containing source filename for the files such as:
   <source.zip>, <source.version>, etc.

4. Enter the following command to build the ORIGIN dataset:

```bash
mvn clean install -U "-DMaven.build.cache.enable=false"
```

5. Enter the following command to deploy the ORIGIN dataset to Nexus:

```bash
mvn deploy -f loinc-origin -DdeployToNexus=true -Dmaven.deploy.skip=true -Dmaven.build.cache.enabled=false -Ptinkarbuild -DrepositoryId=nexus-snapshot
```

6. On Nexus, you will find the artifact at the following maven coordinates:

```bash
<dependency>
  <groupId>dev.ikm.data.loinc</groupId>
  <artifactId>loinc-origin</artifactId>
  <version>Loinc_2.81+1.0.0-20250911.162901-1</version>
</dependency>
```
