from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import yaml
from ceci.config import StageParameter
from rail.core.stage import RailPipeline
from rail.utils import catalog_utils

from .configurable import Configurable

if TYPE_CHECKING:
    from .project import RailProject


def inform_input_callback(
    project: RailProject,
    pipeline_name: str,
    sink_dir: str,  # pylint: disable=unused-argument
    **kwargs: Any,
) -> dict[str, str]:
    """Make dict of input tags and paths for the inform pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    sink_dir: str
        Path to output directory

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...

    Returns
    -------
    dict[str, str]:
        Dictionary of input file tags and paths
    """
    pipeline_info = project.get_pipeline(pipeline_name)
    input_files = {}
    input_file_tags = pipeline_info["input_file_templates"]
    flavor = kwargs.pop("flavor", "baseline")
    for key, val in input_file_tags.items():
        input_file_flavor = val.get("flavor", flavor)
        input_files[key] = project.get_file_for_flavor(
            input_file_flavor, val["tag"], **kwargs
        )
    return input_files


def inform_sompz_input_callback(
    project: RailProject,
    pipeline_name: str,
    sink_dir: str,  # pylint: disable=unused-argument
    **kwargs: Any,
) -> dict[str, str]:
    """Make dict of input tags and paths for the inform pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    sink_dir: str
        Path to output directory

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...

    Returns
    -------
    dict[str, str]:
        Dictionary of input file tags and paths
    """
    pipeline_info = project.get_pipeline(pipeline_name)
    input_files = {}
    input_file_tags = pipeline_info["input_file_templates"]
    flavor = kwargs.pop("flavor", "baseline")
    for key, val in input_file_tags.items():
        input_file_flavor = val.get("flavor", flavor)
        input_files[key] = project.get_file_for_flavor(
            input_file_flavor, val["tag"], **kwargs
        )
    return input_files


def estimate_input_callback(
    project: RailProject,
    pipeline_name: str,
    sink_dir: str,
    **kwargs: Any,
) -> dict[str, str]:
    """Make dict of input tags and paths for the estimate pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    sink_dir: str
        Path to output directory

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...

    Returns
    -------
    dict[str, str]:
        Dictionary of input file tags and paths
    """
    pipeline_info = project.get_pipeline(pipeline_name)
    input_files = {}
    input_file_tags = pipeline_info["input_file_templates"]
    kwcopy = kwargs.copy()
    flavor = kwcopy.pop("flavor", "baseline")
    local_input_tag = kwcopy.pop("input_tag", None)
    if local_input_tag:
        input_files["sink_dir"] = os.path.join(sink_dir, local_input_tag)
    else:
        input_files["sink_dir"] = sink_dir
    for key, val in input_file_tags.items():
        input_file_flavor = kwargs.get("flavor", val.get("flavor", "baseline"))
        input_tag = kwargs.get("input_tag", val["tag"])
        input_files[key] = project.get_file_for_flavor(
            input_file_flavor,
            input_tag,
            **kwcopy,
        )

    pz_algorithms = project.get_pzalgorithms()
    for pz_algo_ in pz_algorithms.keys():
        input_files[f"model_{pz_algo_}"] = os.path.join(
            project.get_path("ceci_output_dir", flavor=input_file_flavor, **kwcopy),
            f"model_inform_{pz_algo_}.pkl",
        )
    return input_files


def estimate_sompz_input_callback(
    project: RailProject,
    pipeline_name: str,
    sink_dir: str,
    **kwargs: Any,
) -> dict[str, str]:
    """Make dict of input tags and paths for the estimate pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    sink_dir: str
        Path to output directory

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...

    Returns
    -------
    dict[str, str]:
        Dictionary of input file tags and paths
    """
    pipeline_info = project.get_pipeline(pipeline_name)
    input_files = {}
    input_file_tags = pipeline_info["input_file_templates"]
    kwcopy = kwargs.copy()
    flavor = kwcopy.pop("flavor", "baseline")
    local_input_tag = kwcopy.pop("input_tag", None)
    if local_input_tag:
        input_files["sink_dir"] = os.path.join(sink_dir, local_input_tag)
    else:
        input_files["sink_dir"] = sink_dir
    for key, val in input_file_tags.items():
        input_file_flavor = kwargs.get("flavor", val.get("flavor", "baseline"))
        input_tag = kwargs.get("input_tag", val["tag"])
        input_files[key] = project.get_file_for_flavor(
            input_file_flavor,
            input_tag,
            **kwcopy,
        )

    for field_ in ['wide', 'deep']:
        input_files[f"{field_}_model"] = os.path.join(
            project.get_path("ceci_output_dir", flavor=input_file_flavor, **kwcopy),
            f"model_som_informer_{field_}.pkl",
        )
    return input_files


def evaluate_input_callback(
    project: RailProject,
    pipeline_name: str,
    sink_dir: str,
    **kwargs: Any,
) -> dict[str, str]:
    """Make dict of input tags and paths for the evalute pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    sink_dir: str
        Path to output directory

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...

    Returns
    -------
    dict[str, str]:
        Dictionary of input file tags and paths
    """
    pipeline_info = project.get_pipeline(pipeline_name)
    input_files = {}
    input_file_tags = pipeline_info["input_file_templates"]
    flavor = kwargs.pop("flavor", "baseline")
    for key, val in input_file_tags.items():
        input_file_flavor = val.get("flavor", flavor)
        input_files[key] = project.get_file_for_flavor(
            input_file_flavor, val["tag"], **kwargs
        )

    pdfs_dir = sink_dir
    pz_algorithms = project.get_pzalgorithms()
    for pz_algo_ in pz_algorithms.keys():
        input_files[f"input_evaluate_{pz_algo_}"] = os.path.join(
            pdfs_dir, f"estimate_output_{pz_algo_}.hdf5"
        )
    return input_files


def pz_input_callback(
    project: RailProject,
    pipeline_name: str,
    sink_dir: str,  # pylint: disable=unused-argument
    **kwargs: Any,
) -> dict[str, str]:
    """Make dict of input tags and paths for the pz pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    sink_dir: str
        Path to output directory

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...

    Returns
    -------
    dict[str, str]:
        Dictionary of input file tags and paths
    """
    pipeline_info = project.get_pipeline(pipeline_name)
    input_files = {}
    input_file_tags = pipeline_info["input_file_templates"]
    flavor = kwargs.pop("flavor")
    for key, val in input_file_tags.items():
        input_file_flavor = val.get("flavor", flavor)
        input_files[key] = project.get_file_for_flavor(
            input_file_flavor, val["tag"], **kwargs
        )
    return input_files


def tomography_input_callback(
    project: RailProject,
    pipeline_name: str,
    sink_dir: str,
    **kwargs: Any,
) -> dict[str, str]:
    """Make dict of input tags and paths for the tomography pipeline

    Parameters
    ----------
    project: RailProject
        Object with project configuration

    pipeline_name: str
        Name of the pipeline to run

    sink_dir: str
        Path to output directory

    kwargs: Any
        Additional parameters to specify pipeline, e.g., flavor, selection, ...

    Returns
    -------
    dict[str, str]:
        Dictionary of input file tags and paths
    """
    pipeline_info = project.get_pipeline(pipeline_name)
    input_files = {}
    input_file_tags = pipeline_info["input_file_templates"]
    flavor = kwargs.pop("flavor")
    selection = kwargs.get("selection")
    for key, val in input_file_tags.items():
        input_file_flavor = val.get("flavor", flavor)
        input_files[key] = project.get_file_for_flavor(
            input_file_flavor, val["tag"], selection=selection
        )

    pdfs_dir = sink_dir
    pz_algorithms = project.get_pzalgorithms()
    for pz_algo_ in pz_algorithms.keys():
        input_files[f"input_{pz_algo_}"] = os.path.join(
            pdfs_dir, f"output_estimate_{pz_algo_}.hdf5"
        )

    return input_files


def truth_to_observed_convert_commands(
    sink_dir: str, **_kwargs: Any
) -> list[list[str]]:
    convert_command = [
        "tables-io",
        "convert",
        "--input",
        f"{sink_dir}/output_dereddener_errors.pq",
        "--output",
        f"{sink_dir}/output.hdf5",
    ]
    convert_commands = [convert_command]
    return convert_commands


def prepare_convert_commands(sink_dir: str, **_kwargs: Any) -> list[list[str]]:
    convert_command = [
        "tables-io",
        "convert",
        "--input",
        f"{sink_dir}/output_deredden.pq",
        "--output",
        f"{sink_dir}/output.hdf5",
    ]
    convert_commands = [convert_command]
    return convert_commands


def photometric_errors_convert_commands(
    sink_dir: str, **_kwargs: Any
) -> list[list[str]]:
    convert_command = [
        "tables-io",
        "convert",
        "--input",
        f"{sink_dir}/output_dereddener_errors.pq",
        "--output",
        f"{sink_dir}/output.hdf5",
    ]
    convert_commands = [convert_command]
    return convert_commands


def spectroscopic_selection_convert_commands(
    sink_dir: str, **kwargs: Any
) -> list[list[str]]:
    convert_commands = []
    spec_selections = kwargs.get("spec_selections")
    assert isinstance(spec_selections, list)
    for spec_selection_ in spec_selections:
        convert_command = [
            "tables-io",
            "convert",
            "--input",
            f"{sink_dir}/output_select_{spec_selection_}.pq",
            "--output",
            f"{sink_dir}/output_select_{spec_selection_}.hdf5",
        ]
        convert_commands.append(convert_command)
    return convert_commands


def blending_convert_commands(sink_dir: str, **_kwargs: Any) -> list[list[str]]:
    convert_command = [
        "tables-io",
        "convert",
        "--input",
        f"{sink_dir}/output_blended.pq",
        "--output",
        f"{sink_dir}/output_blended.hdf5",
    ]
    convert_commands = [convert_command]
    return convert_commands


INPUT_CALLBACK_DICT = dict(
    inform=inform_input_callback,
    estimate=estimate_input_callback,
    evaluate=evaluate_input_callback,
    inform_sompz=inform_sompz_input_callback,
    estimate_sompz=estimate_sompz_input_callback,
    pz=pz_input_callback,
    tomography=tomography_input_callback,
)


CATALOG_CONVERT_COMMANDS_DICT = dict(
    truth_to_observed=truth_to_observed_convert_commands,
    prepare=prepare_convert_commands,
    photometric_errors=photometric_errors_convert_commands,
    spec_selection=spectroscopic_selection_convert_commands,
    blending=blending_convert_commands,
)


class RailPipelineTemplate(Configurable):
    """Simple class for holding a pipeline configuraiton"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Pipeline name"),
        pipeline_class=StageParameter(
            str,
            None,
            fmt="%s",
            required=True,
            msg="Full class name for Pipeline",
        ),
        input_catalog_template=StageParameter(
            str,
            "",
            fmt="%s",
            msg="Template to use for input catalog",
        ),
        output_catalog_template=StageParameter(
            str,
            "",
            fmt="%s",
            msg="Template to use for output catalog",
        ),
        input_catalog_basename=StageParameter(
            str,
            "",
            fmt="%s",
            msg="Basename to use for input catalog",
        ),
        input_file_templates=StageParameter(
            dict,
            {},
            fmt="%s",
            msg="Templates to use for input files",
        ),
        kwargs=StageParameter(
            dict,
            {},
            fmt="%s",
            msg="Keywords to provide Pipeline c'tor",
        ),
    )

    yaml_tag = "PipelineTemplate"

    def __init__(self, **kwargs: Any) -> None:
        """C'tor

        Parameters
        ----------
        **kwargs: Any
            Configuration parameters for this RailPipelineTemplate, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

    def __repr__(self) -> str:
        return f"{self.config.pipeline_class}"

    def make_instance(
        self,
        project: RailProject,
        flavor: str,
        pipeline_overrides: dict[str, Any],
    ) -> RailPipelineInstance:
        kwargs_copy = self.config.kwargs.copy()
        if "kwargs" in pipeline_overrides:  # pragma: no cover
            kwargs_update = pipeline_overrides.pop("kwargs", {})
            kwargs_copy.update(**kwargs_update)

        path = project.get_path(
            "pipeline_path", pipeline=self.config.name, flavor=flavor
        )
        return RailPipelineInstance(
            name=f"{self.config.name}_{flavor}",
            path=path,
            pipeline_template=self.config.name,
            flavor=flavor,
            kwargs=kwargs_copy,
            pipeline_overrides=pipeline_overrides,
        )


class RailPipelineInstance(Configurable):
    """Simple class for holding a pipeline configuraiton"""

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Pipeline name"),
        path=StageParameter(str, None, fmt="%s", msg="Path to pipeline file"),
        pipeline_template=StageParameter(
            str,
            None,
            fmt="%s",
            required=True,
            msg="Name of PipelineTemplate to use",
        ),
        flavor=StageParameter(str, None, fmt="%s", msg="Pipeline flavor"),
        kwargs=StageParameter(
            dict,
            {},
            fmt="%s",
            msg="Keywords to provide Pipeline c'tor",
        ),
        pipeline_overrides=StageParameter(
            dict,
            {},
            fmt="%s",
            msg="Parameters to override from template",
        ),
    )

    yaml_tag = "PipelineInstance"

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailPipelineInstance, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

    def __repr__(self) -> str:
        return f"{self.config.pipeline_template} {self.config.path}"

    def build(
        self,
        project: RailProject,
    ) -> int:
        output_dir = project.get_common_path("project_scratch_dir")
        pipeline_template = project.get_pipeline(self.config.pipeline_template)
        pipeline_class = pipeline_template.config.pipeline_class

        pipe_out_dir = os.path.dirname(self.config.path)
        try:
            os.makedirs(pipe_out_dir)
        except FileExistsError:
            pass

        pipeline_kwargs = self.config.kwargs.copy()

        if self.config.pipeline_overrides:  # pragma: no cover
            copy_overrides = self.config.pipeline_overrides.copy()

            stages_config = os.path.join(
                pipe_out_dir, f"{self.config.name}_overrides.yml"
            )

            kwarg_overrides = copy_overrides.pop("kwargs", {})
            pipeline_kwargs.update(**kwarg_overrides)

            with open(stages_config, "w", encoding="utf-8") as fout:
                yaml.dump(copy_overrides, fout)
        else:
            stages_config = None

        for key, val in pipeline_kwargs.items():
            if key == "selectors":
                temp_dict = project.get_spec_selections()
            elif key == "algorithms":
                temp_dict = project.get_pzalgorithms()
            elif key == "classifiers":
                temp_dict = project.get_classifiers()
            elif key == "summarizers":
                temp_dict = project.get_summarizers()
            elif key == "error_models":
                temp_dict = project.get_error_models()
            else:
                continue
            if "all" in val:
                pipeline_kwargs[key] = temp_dict
            else:
                pipeline_kwargs[key] = {
                    algo_name_: temp_dict[algo_name_] for algo_name_ in val
                }

        catalog_tag = project.get_flavor(self.config.flavor).get("catalog_tag", None)
        if catalog_tag:
            catalog_utils.apply_defaults(catalog_tag)

        tokens = pipeline_class.split(".")
        module = ".".join(tokens[:-1])
        class_name = tokens[-1]
        log_dir = f"{pipe_out_dir}/logs/{self.config.name}"

        print(f"Writing {self.config.path}")

        __import__(module)
        RailPipeline.build_and_write(
            class_name,
            self.config.path,
            None,
            stages_config,
            output_dir,
            log_dir,
            **pipeline_kwargs,
        )

        return 0

    def get_input_files(
        self,
        project: RailProject,
        **kwargs: Any,
    ) -> dict[str, str]:
        """Get the input files needed to run this instandce

        Parameters
        ----------
        project: RailProject
            Object with project configuration

        **kwargs:
            Additional parameters to specify pipeline, e.g., flavor, selection, ...

        Returns
        -------
        dict[str, str]
            Input files, keyed by label
        """

        pipeline_name = self.config.pipeline_template
        sink_dir = project.get_path(
            "ceci_output_dir", flavor=self.config.flavor, **kwargs
        )
        input_callback = INPUT_CALLBACK_DICT[pipeline_name]
        input_files = input_callback(
            project, pipeline_name, sink_dir, flavor=self.config.flavor, **kwargs
        )
        input_files.setdefault("sink_dir", sink_dir)
        return input_files

    def make_pipeline_single_input_command(
        self,
        project: RailProject,
        **kwargs: Any,
    ) -> list[str]:
        """Make the command to run a single instance

        Parameters
        ----------
        project: RailProject
            Object with project configuration

        **kwargs:
            Additional parameters to specify pipeline, e.g., flavor, selection, ...

        Returns
        -------
        list[str]
            Commands to run
        """
        pipeline_path = self.config.path
        pipeline_config = pipeline_path.replace(".yaml", "_config.yml")
        input_files = self.get_input_files(project, **kwargs)
        sink_dir = input_files.pop("sink_dir")
        command_line = project.generate_ceci_command(
            pipeline_path=pipeline_path,
            config=pipeline_config,
            inputs=input_files,
            output_dir=sink_dir,
            log_dir=f"{sink_dir}/logs",
        )
        return command_line

    def make_pipeline_catalog_commands(
        self,
        project: RailProject,
        **kwargs: Any,
    ) -> list[tuple[list[list[str]], str]]:
        """Make the command to run a pipeline on an entire catalog

        Parameters
        ----------
        project: RailProject
            Object with project configuration

        kwargs: Any
            Additional parameters to specify pipeline, e.g., flavor, selection, ...

        Returns
        -------
        commands: list[tuple[list[list[str]], str]]
            Commands to run
        """
        pipeline_name = self.config.pipeline_template
        pipeline_info = project.get_pipeline(pipeline_name)
        flavor = self.config.flavor
        pipeline_path = project.get_path(
            "pipeline_path", pipeline=pipeline_name, flavor=flavor, **kwargs
        )

        catalog_convert_commands_function = CATALOG_CONVERT_COMMANDS_DICT[pipeline_name]

        source_catalog_files = project.get_catalog_files(
            pipeline_info.config.input_catalog_template,
            basename=pipeline_info.config.input_catalog_basename,
            flavor=self.config.flavor,
            **kwargs,
        )
        sink_catalog_files = project.get_catalog_files(
            pipeline_info.config.output_catalog_template,
            basename="output.hdf5",
            flavor=self.config.flavor,
            **kwargs,
        )

        all_commands: list[tuple[list[list[str]], str]] = []

        selection = kwargs["selection"]

        for source_catalog, sink_catalog in zip(
            source_catalog_files, sink_catalog_files
        ):
            sink_dir = os.path.dirname(sink_catalog)
            script_path = os.path.join(
                sink_dir,
                f"submit_{pipeline_name}_{selection}_{flavor}.sh",
            )
            ceci_commands = project.generate_ceci_command(
                pipeline_path=pipeline_path,
                config=pipeline_path.replace(".yaml", "_config.yml"),
                inputs=dict(input=source_catalog),
                output_dir=sink_dir,
                log_dir=sink_dir,
            )
            convert_commands = catalog_convert_commands_function(
                sink_dir,
                **kwargs,
            )
            iter_commands = [
                ["mkdir", "-p", f"{sink_dir}"],
                ceci_commands,
                *convert_commands,
            ]
            all_commands.append((iter_commands, script_path))

        return all_commands
