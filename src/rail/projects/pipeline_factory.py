from __future__ import annotations

from typing import Any
import os
import yaml

from .pipeline_holder import RailPipelineTemplate, RailPipelineInstance


class RailPipelineFactory:
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

    _instance: RailPipelineFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        self._pipeline_templates: dict[str, RailPipelineTemplate] = {}
        self._pipeline_instances: dict[str, RailPipelineInstance] = {}

    @classmethod
    def instance(cls) -> RailPipelineFactory:
        """Return the singleton instance of the factory"""
        if cls._instance is None:
            cls._instance = RailPipelineFactory()
        return cls._instance

    @classmethod
    def clear(cls) -> None:
        """Clear the contents of the factory"""
        if cls._instance is None:
            return
        cls._instance.clear_instance()

    @classmethod
    def print_contents(cls) -> None:
        """Print the contents of the factory"""
        if cls._instance is None:
            cls._instance = RailPipelineFactory()
        cls._instance.print_instance_contents()

    @classmethod
    def load_yaml(cls, yaml_file: str) -> None:
        """Load a yaml file

        Parameters
        ----------
        yaml_file: str
            File to read and load

        Notes
        -----
        See class helpstring for yaml format
        """
        if cls._instance is None:
            cls._instance = RailPipelineFactory()
        cls._instance.load_instance_yaml(yaml_file)

    @classmethod
    def load_yaml_tag(cls, pipelines_config: list[dict[str, Any]]) -> None:
        """Load from a yaml tag

        Parameters
        ----------
        pipelines_config: list[dict[str, Any]]
            Yaml tag used to load

        Notes
        -----
        See class helpstring for yaml format
        """
        if cls._instance is None:
            cls._instance = RailPipelineFactory()
        cls._instance.load_pipelines_from_yaml_tag(pipelines_config)

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

    def clear_instance(self) -> None:
        """Clear out the contents of the factory"""
        self._pipeline_templates.clear()
        self._pipeline_instances.clear()

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

    def _make_pipeline_instance(self, **kwargs: Any) -> RailPipelineInstance:
        try:
            name = kwargs["name"]
        except KeyError as missing_key:
            raise KeyError(
                "PipelineInstance yaml block does not contain name for pipeline_instance: "
                f"{list(kwargs.keys())}"
            ) from missing_key
        if name in self._pipeline_instances:  # pragma: no cover
            raise KeyError(f"RailPipelineInstance {name} is already defined")
        pipeline_instance = RailPipelineInstance(**kwargs)
        self._pipeline_instances[name] = pipeline_instance
        return pipeline_instance

    def _make_pipeline_template(self, **kwargs: Any) -> RailPipelineTemplate:
        try:
            name = kwargs["name"]
        except KeyError as missing_key:
            raise KeyError(
                "PipelineTemplate yaml block does not contain name for pipeline_template: "
                f"{list(kwargs.keys())}"
            ) from missing_key
        if name in self._pipeline_templates:  # pragma: no cover
            raise KeyError(f"RailPipelineTemplate {name} is already defined")
        pipeline_template = RailPipelineTemplate(**kwargs)
        self._pipeline_templates[name] = pipeline_template
        return pipeline_template

    def load_pipeline_template_from_yaml_tag(
        self, pipeline_template_config: dict[str, Any]
    ) -> None:
        """Load a dataset from a Dataset tag in yaml

        Paramters
        ---------
        pipeline_template_config: dict[str, Any]
            Yaml data in question
        """
        self._make_pipeline_template(**pipeline_template_config)

    def load_pipeline_instance_from_yaml_tag(
        self, pipeline_instance_config: dict[str, Any]
    ) -> None:
        """Load a dataset from a Dataset tag in yaml

        Paramters
        ---------
        pipeline_instance_config: dict[str, Any]
            Yaml data in question
        """
        self._make_pipeline_instance(**pipeline_instance_config)

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
        for pipelines_item in pipelines_config:
            if "PipelineTemplate" in pipelines_item:
                pipeline_template_config = pipelines_item["PipelineTemplate"]
                self.load_pipeline_template_from_yaml_tag(pipeline_template_config)
            elif "PipelineInstance" in pipelines_item:
                pipeline_instance_config = pipelines_item["PipelineInstance"]
                self.load_pipeline_instance_from_yaml_tag(pipeline_instance_config)
            else:  # pragma: no cover
                good_keys = ["PipelineTemplate", "PipelineInstance"]
                raise KeyError(
                    f"Expecting one of {good_keys} not: {pipelines_item.keys()})"
                )

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
