
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.aksw.ckan.processors.ckan;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.aksw.ckan_deploy.core.DcatCkanDeployUtils;
import org.aksw.ckan_deploy.core.DcatExpandUtils;

import eu.trentorise.opendata.jackan.CkanClient;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "ckan", "rdf", "dcat" })
@CapabilityDescription("Nifi Processor that will upload a distribution based on it dcat description.")
public class DcatUploadProcessor extends AbstractProcessor {

    private static final PropertyDescriptor CKAN_url = new PropertyDescriptor.Builder().name("CKAN_url")
            .displayName("CKAN Url").description("Hostname of the CKAN instance to write to")
            .addValidator(StandardValidators.URL_VALIDATOR).required(true).build();
    private static final PropertyDescriptor api_key = new PropertyDescriptor.Builder().name("Api_Key")
            .displayName("File Api_Key").description("Api Key to be used to interact with CKAN")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).sensitive(true).build();
    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder().name("Directory").description(
            "The directory to which files should be written. You may use expression language such as /aa/bb/${path}")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor CREATE_DIRS = new PropertyDescriptor.Builder()
            .name("Create Missing Directories")
            .description(
                    "If true, then missing destination directories will be created. If false, flowfiles are penalized and sent to failure.")
            .required(true).allowableValues("true", "false").defaultValue("true").build();

    private static final Relationship REL_SUCCESS = new Relationship.Builder().name("SUCCESS")
            .description("Success relationship").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description(
            "Files that could not be written to the output directory for some reason are transferred to this relationship")
            .build();
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CKAN_url);
        descriptors.add(api_key);
        descriptors.add(DIRECTORY);
        descriptors.add(CREATE_DIRS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final ComponentLog logger = getLogger();

        final Path configuredRootDirPath = Paths
                .get(context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue());

        // final AtomicReference<Model> dcatDataset = new AtomicReference<>();
        try {
            final Path rootDirPath = configuredRootDirPath;

            if (!Files.exists(rootDirPath)) {
                if (context.getProperty(CREATE_DIRS).asBoolean()) {
                    Files.createDirectories(rootDirPath);
                } else {
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                    logger.error(
                            "Penalizing {} and routing to 'failure' because the output directory {} does not exist and Processor is "
                                    + "configured not to create missing directories",
                            new Object[] { flowFile, rootDirPath });
                    return;
                }
            }
        } catch (final Throwable t) {
            flowFile = session.penalize(flowFile);
            logger.error("Penalizing {} and transferring to failure due to {}", new Object[] { flowFile, t });
            session.transfer(flowFile, REL_FAILURE);
        }

        CkanClient ckanClient = new CkanClient(context.getProperty("CKAN_url").getValue(),
                context.getProperty("Api_Key").getValue());

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                Dataset dataset = DatasetFactory.create();
                RDFDataMgr.read(dataset, in, Lang.TURTLE);
                Model dcatDataset = DcatExpandUtils.export(dataset, configuredRootDirPath);
                DcatCkanDeployUtils.deploy(ckanClient, dcatDataset, null, false);
            }
        });
        flowFile = session.write(flowFile, new OutputStreamCallback() {

            @Override
            public void process(OutputStream out) throws IOException {
                // RDFDataMgr.write(out, dcatDataset.get(), Lang.TURTLE);
            }
        });
        session.transfer(flowFile, REL_SUCCESS);
    }

}
