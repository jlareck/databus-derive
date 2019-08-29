# Databus-derive-maven-plugin

Extension to the general [databus-maven-plugin](https://github.com/dbpedia/databus-maven-plugin), used to download and clean datasets already released on the [databus](https://databus.dbpedia.org).

## Setup 

Maven install, need until added to maven repository.
```
git clone https://github.com/dbpedia/databus-derive.git
cd databus-derive
mvn clean install
```

Add the following plugin entry to the `BaseBuild(<build>)` element in your maven pom.
```
  <build>
  ...
    <plugins>
      <plugin>
        <groupId>org.dbpedia.databus</groupId>
        <artifactId>databus-derive-maven-plugin</artifactId>
        <version>1.0-SNAPSHOT</version>
        <executions>
          <execution>
            <!--phase>initialize</phase-->
            <goals>
              <goal>clone-parse</goal>
            </goals>
          </execution>
        </executions>
        <configuration>
          <versions>
            <version>https://databus.dbpedia.org/dbpedia/enrichment/mappingbased-literals/2019.03.01</version>
          </versions>
          <targetDirectory>./</targetDirectory>
          <reportDirectory>.reports</reportDirectory>
          <downloadDirectory>.download</downloadDirectory>
          <deleteDownloadCache>false</deleteDownloadCache>
          <skipParsing>false</skipParsing>
        </configuration>
      </plugin>
    </plugins>
  </build>
```

Note that you can also use a variable for configuration
```
 <properties>
	<!-- used for derive plugin, can be anything really -->
        <databus.marvinversion>2019.08.01</databus.marvinversion>
 </properties>

 <configuration>
          <versions>
            <version>https://databus.dbpedia.org/marvin/mappings/geo-coordinates-mappingbased/${databus.marvinversion}</version>
            <version>https://databus.dbpedia.org/marvin/mappings/instance-types/${databus.marvinversion}</version>
            <version>https://databus.dbpedia.org/marvin/mappings/mappingbased-literals/${databus.marvinversion}</version>
            <version>https://databus.dbpedia.org/marvin/mappings/mappingbased-objects-uncleaned/${databus.marvinversion}</version>
            <version>https://databus.dbpedia.org/marvin/mappings/specific-mappingbased-properties/${databus.marvinversion}</version>
          </versions>
 </configuration>
```

## Usage

To derive and parse released datasets add entries under the `configuration/versions` section.

Execute the plugin.

```
# normal
export MAVEN_OPTS="-Xmx24000m" 
# for 64Cores ~ 64GB to parse 5GB bz2
export MAVEN_OPTS="-Xmx64000m" 

mvn databus-derive:clone-parse
```

## Stanadalone Execution

It is also possible to just create cleaned triples and pareslogs.
```
mvn scala:run -Dlauncher=flatRdfParser -DaddArgs="$flat-rdf-in|-o|$flat-rdf-out|-r|$report-out"
```
or
```
./flatRdfParser <flat-rdf-in> <flat-rdf-out> <report-out>
```
for help add `--help`
