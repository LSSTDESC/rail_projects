from __future__ import annotations

from .factory_mixin import RailFactoryMixin
from .pipeline_holder import RailPipelineInstance, RailPipelineTemplate


class RailPipelineFactory(RailFactoryMixin):
    """Factory class to make pipelines

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:

    .. highlight:: yaml
    .. code-block:: yaml

      Pipelines:
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

    """

    yaml_tag: str = "Pipelines"

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
        RailProjectPipelineTemplate:
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
        RailProjectPipelineInstance:
            pipeline instance in question
        """
        try:
            return cls.instance().pipeline_instances[name]
        except KeyError as msg:
            raise KeyError(
                f"RailPipelineInstance named {name} not found in RailPipelineInstance "
                f"{list(cls.instance().pipeline_instances.keys())}"
            ) from msg

    @classmethod
    def add_pipeline_instance(cls, pipeline_instance: RailPipelineInstance) -> None:
        """Add a particular PipelineInstance to the factory"""
        cls.instance().add_to_dict(pipeline_instance)

    @classmethod
    def add_pipeline_template(cls, pipeline_template: RailPipelineTemplate) -> None:
        """Add a particular PipelineTemplate to the factory"""
        cls.instance().add_to_dict(pipeline_template)

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
        RailFactoryMixin.print_instance_contents(self)
