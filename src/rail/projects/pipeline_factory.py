from __future__ import annotations

from typing import Any
import os
import yaml

from .pipeline_holder import RailPipelineTemplate, RailPipelineInstance
from .factory_mixin import RailFactoryMixin


class RailPipelineFactory(RailFactoryMixin):
    """Factory class to make pipelines

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:

    PipelineTemplates:
      - PipelineTemplate:
            name: pz:
            pipeline_class: rail.pipelines.estimation.pz_all.PzPipeline
                input_catalog_template: degraded
            output_catalog_template: degraded
            input_file_templates:
                input_train:
                    flavor: baseline
                    tag: train
                input_test:
                    flavor: baseline
                    tag: test
            kwargs:
                algorithms: ['all']

    Pipe
    """

    client_classes = [RailPipelineTemplate, RailPipelineInstance]

    _instance: RailPipelineFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        RailFactoryMixin.__init__(self)
        self._pipeline_templates = self.add_dict(RailPipelineTemplate)
        self._pipeline_instances = self.add_dict(RailPipelineInstance)

    @classmethod
    def get_pipeline_templates(cls) -> dict[str, RailPipelineTemplate]:
        """Return the dict of all the pipeline templates"""
        return cls.instance().pipeline_templates

    @classmethod
    def get_pipeline_template_names(cls) -> list[str]:
        """Return the names of the pipeline templates"""
        return list(cls.instance().pipeline_templates.keys())

    @classmethod
    def get_pipeline_instances(cls) -> dict[str, RailPipelineInstance]:
        """Return the dict of all the pipeline instances"""
        return cls.instance().pipeline_instances

    @classmethod
    def get_pipeline_instance_names(cls) -> list[str]:
        """Return the names of the pipeline instances lists"""
        return list(cls.instance().pipeline_instances.keys())

    @classmethod
    def get_pipeline_template(cls, name: str) -> RailPipelineTemplate:
        """Get pipeline templates by it's assigned name

        Parameters
        ----------
        name: str
            Name of the pipeline templates to return

        Returns
        -------
        pipeline_templates: RailProjectPipelineTemplate
            pipeline templates in question
        """
        try:
            return cls.instance().pipeline_templates[name]
        except KeyError as msg:
            raise KeyError(
                f"RailPipelineTemplate named {name} not found in RailPipelineFactory "
                f"{list(cls.instance().pipeline_templates.keys())}"
            ) from msg

    @classmethod
    def get_pipeline_instance(cls, name: str) -> RailPipelineInstance:
        """Get a pipeline instance by its assigned name

        Parameters
        ----------
        name: str
            Name of the pipeline instance list to return

        Returns
        -------
        pipeline_instance: RailProjectPipelineInstance
            pipeline instance in question
        """
        try:
            return cls.instance().pipeline_instances[name]
        except KeyError as msg:
            raise KeyError(
                f"RailPipelineInstance named {name} not found in RailPipelineInstance "
                f"{list(cls.instance().pipeline_instances.keys())}"
            ) from msg

    @property
    def pipeline_templates(self) -> dict[str, RailPipelineTemplate]:
        """Return the dictionary of pipeline templates"""
        return self._pipeline_templates

    @property
    def pipeline_instances(self) -> dict[str, RailPipelineInstance]:
        """Return the dictionary of pipeline instances"""
        return self._pipeline_instances

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("----------------")
        print("Pipelines:")
        print("----------------")
        print("PipelineTemplates:")
        for template_name, pipeline_template in self.pipeline_templates.items():
            print(f"  {template_name}: {pipeline_template}")
        print("----------------")
        print("PipelineInstances:")
        for template_name, pipeline_instance in self.pipeline_instances.items():
            print(f"  {template_name}: {pipeline_instance}")

    def add_pipeline_instance(self, pipeline_instance: RailPipelineInstance) -> None:
        self.add_to_dict(pipeline_instance)

    def add_pipeline_template(self, pipeline_template: RailPipelineTemplate) -> None:
        self.add_to_dict(pipeline_template)

    def load_pipelines_from_yaml_tag(
        self,
        pipelines_config: list[dict[str, Any]],
    ) -> None:
        """Read a yaml "Pipelines" tag and load the factory accordingy

        Parameters
        ----------
        pipelines_config: list[dict[str, Any]]
            Yaml tag to load

        Notes
        -----
        See class description for yaml file syntax
        """
        self.load_instance_yaml_tag(pipelines_config)

    def load_instance_yaml(self, yaml_file: str) -> None:
        """Read a yaml file and load the factory accordingly

        Parameters
        ----------
        yaml_file: str
            File to read

        Notes
        -----
        See class description for yaml file syntax
        """
        with open(os.path.expandvars(yaml_file), encoding="utf-8") as fin:
            yaml_data = yaml.safe_load(fin)

        try:
            pipelines_config = yaml_data["Pipelines"]
        except KeyError as missing_key:
            raise KeyError(
                f"Did not find key Pipelines in {yaml_file}"
            ) from missing_key

        self.load_pipelines_from_yaml_tag(pipelines_config)
