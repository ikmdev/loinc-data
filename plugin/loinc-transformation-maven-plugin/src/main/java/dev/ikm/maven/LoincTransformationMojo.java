package dev.ikm.maven;

import dev.ikm.tinkar.common.service.CachingService;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.composer.Composer;
import dev.ikm.tinkar.entity.EntityService;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;

import org.apache.maven.plugins.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@Mojo(name = "run-loinc-transformation", defaultPhase = LifecyclePhase.INSTALL)
public class LoincTransformationMojo extends AbstractMojo {
    private static final Logger LOG = LoggerFactory.getLogger(LoincTransformationMojo.class.getSimpleName());

    @Parameter(property = "partCsv", defaultValue=${project.basedir/src/main/resources/part.csv, required = true})
    private File partCsv;

    @Parameter(property = "loincCsv", defaultValue=${project.basedir/src/main/resources/loinc.csv, required = true})
    private File loincCsv;

    @Override
    public void execute() throws MojoExecutionException {
        LOG.info("########## Loinc Transformer Starting...");
    }
}