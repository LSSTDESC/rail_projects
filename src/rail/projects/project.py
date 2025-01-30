from __future__ import annotations

import os
import itertools
from typing import Any

import yaml

from ceci.config import StageParameter
from rail.plotting.configurable import Configurable
from rail.utils import catalog_utils
from rail.core.stage import RailPipeline

from . import name_utils, library
from .catalog_template import RailProjectCatalogTemplate
from .pipeline_holder import RailPipelineTemplate
from .selection_factory import RailSelection
from .file_template import RailProjectFileTemplate
from .algorithm_factory import RailAlgorithmFactory
from .catalog_factory import RailCatalogFactory
from .pipeline_factory import RailPipelineFactory
from .project_file_factory import RailProjectFileFactory
from .selection_factory import RailSelectionFactory


class RailFlavor(Configurable):
    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Flavor name"),
        catalog_tag=StageParameter(
            str, None, fmt="%s", msg="tag for catalog being used"
        ),
        pipelines=StageParameter(list, ["all"], fmt="%s", msg="pipelines being used"),
        file_aliases=StageParameter(dict, {}, fmt="%s", msg="file aliases used"),
        pipeline_overrides=StageParameter(dict, {}, fmt="%s", msg="file aliases used"),
    )

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailFlavor, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)


class RailProject(Configurable):
    config_options: dict[str, StageParameter] = dict(
        Name=StageParameter(str, None, fmt="%s", required=True, msg="Project name"),
        PathTemplates=StageParameter(
            dict, {}, fmt="%s", required=True, msg="File path templates"
        ),
        CommonPaths=StageParameter(
            dict, {}, fmt="%s", required=True, msg="Paths to shared directories"
        ),
        IterationVars=StageParameter(
            dict, {}, fmt="%s", msg="Iteration variables to use"
        ),
        Catalogs=StageParameter(
            list, ["all"], fmt="%s", msg="Catalog templates to use"
        ),
        Files=StageParameter(list, ["all"], fmt="%s", msg="Catalog templates to use"),
        Pipelines=StageParameter(
            list, ["all"], fmt="%s", msg="Catalog templates to use"
        ),
        Reducers=StageParameter(list, ["all"], fmt="%s", msg="Data reducers to use"),
        Subsamplers=StageParameter(
            list, ["all"], fmt="%s", msg="Data subsamplers to use"
        ),
        Selections=StageParameter(
            list, ["all"], fmt="%s", msg="Data selections to use"
        ),
        PZAlgorithms=StageParameter(
            list, ["all"], fmt="%s", msg="p(z) algorithms to use"
        ),
        SpecSelections=StageParameter(
            list, ["all"], fmt="%s", msg="Spectroscopic selections to use"
        ),
        Classifiers=StageParameter(
            list, ["all"], fmt="%s", msg="Tomographic classifiers to use"
        ),
        Summarizers=StageParameter(
            list, ["all"], fmt="%s", msg="n(z) summarizers to use"
        ),
        ErrorModels=StageParameter(
            list, ["all"], fmt="%s", msg="Photometric ErrorModels to use"
        ),
        Baseline=StageParameter(
            dict, None, fmt="%s", required=True, msg="Baseline analysis configuration"
        ),
        Flavors=StageParameter(list, [], fmt="%s", msg="Analysis variants"),
    )

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailProject, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)
        self.name_factory = name_utils.NameFactory(
            config=self.config,
            templates=self.config.PathTemplates,
            interpolants=self.config.CommonPaths,
        )
        self._iteration_vars: dict[str, list[Any]] | None = None
        self._catalog_templates: dict[str, RailProjectCatalogTemplate] | None = None
        self._file_templates: dict[str, RailProjectFileTemplate] | None = None
        self._pipeline_templates: dict[str, RailPipelineTemplate] | None = None
        self._algorithms: dict[str, dict[str, dict[str, str]]] = {}
        self._selections: dict[str, RailSelection] | None = None
        self._flavors: dict[str, RailFlavor] | None = None

    def __repr__(self) -> str:
        return f"{self.config.Name}"

    @property
    def name(self) -> str:
        return self.config.Name

    @staticmethod
    def load_config(config_file: str) -> RailProject:
        """Create and return a RailProject from a yaml config file"""
        with open(os.path.expandvars(config_file), "r", encoding="utf-8") as fp:
            config_orig = yaml.safe_load(fp)
        includes = config_orig.get("Includes", [])
        # FIXME, make this recursive to allow for multiple layers of includes
        for include_ in includes:
            library.load_yaml(os.path.expandvars(include_))

        project_config = config_orig.get("Project")
        project = RailProject(**project_config)
        return project

    def get_path_templates(self) -> dict:
        """Return the dictionary of templates used to construct paths"""
        return self.name_factory.get_path_templates()

    def get_path(self, path_key: str, **kwargs: Any) -> str:
        """Resolve and return a path using the kwargs as interopolants"""
        return self.name_factory.resolve_path_template(path_key, **kwargs)

    def get_common_paths(self) -> dict:
        """Return the dictionary of common paths"""
        return self.name_factory.get_common_paths()

    def get_common_path(self, path_key: str, **kwargs: Any) -> str:
        """Resolve and return a common path using the kwargs as interopolants"""
        return self.name_factory.resolve_common_path(path_key, **kwargs)

    def get_files(self) -> dict[str, RailProjectFileTemplate]:
        """Return the dictionary of specific file templates"""
        if self._file_templates is not None:
            return self._file_templates
        if "all" in self.config.Files:
            self._file_templates = RailProjectFileFactory.get_file_templates()
        else:
            self._file_templates = {
                key: RailProjectFileFactory.get_file_template(key)
                for key in self.config.Files
            }
        return self._file_templates

    def get_file(self, name: str, **kwargs: Any) -> str:
        """Resolve and return a file using the kwargs as interpolants"""
        files = self.get_files()
        try:
            file_template = files[name]
        except KeyError as missing_key:
            raise KeyError(
                f"file '{name}' not found in {list(files.keys())}"
            ) from missing_key

        path = self.name_factory.resolve_path(
            file_template.config.to_dict(), "path_template", **kwargs
        )
        return path

    def get_flavors(self) -> dict[str, RailFlavor]:
        """Return the dictionary of analysis flavor variants"""
        if self._flavors is not None:
            return self._flavors

        baseline = self.config.Baseline
        self._flavors = dict(baseline=RailFlavor(name="baseline", **baseline))

        flavor_list = self.config.Flavors

        for flavor_item in flavor_list:
            flavor_dict = baseline.copy()
            flavor_dict.update(**flavor_item["Flavor"])
            flavor_name = flavor_dict["name"]
            self._flavors[flavor_name] = RailFlavor(**flavor_dict)

        return self._flavors

    def get_flavor(self, name: str) -> RailFlavor:
        """Resolve the configuration for a particular analysis flavor variant"""
        flavors = self.get_flavors()
        try:
            return flavors[name]
        except KeyError as missing_key:
            raise KeyError(
                f"flavor '{name}' not found in {list(flavors.keys())}"
            ) from missing_key

    def get_file_for_flavor(self, flavor: str, label: str, **kwargs: Any) -> str:
        """Resolve the file associated to a particular flavor and label

        E.g., flavor=baseline and label=train would give the baseline training file
        """
        flavor_dict = self.get_flavor(flavor)
        try:
            file_alias = flavor_dict.config.file_aliases[label]
        except KeyError as msg:
            raise KeyError(f"Label '{label}' not found in flavor '{flavor}'") from msg
        return self.get_file(file_alias, flavor=flavor, label=label, **kwargs)

    def get_file_metadata_for_flavor(self, flavor: str, label: str) -> dict:
        """Resolve the metadata associated to a particular flavor and label

        E.g., flavor=baseline and label=train would give the baseline training metadata
        """
        flavor_dict = self.get_flavor(flavor)
        try:
            file_alias = flavor_dict["file_aliases"][label]
        except KeyError as msg:
            raise KeyError(f"Label '{label}' not found in flavor '{flavor}'") from msg
        return self.get_files()[file_alias]

    def get_selections(self) -> dict[str, RailSelection]:
        """Get the dictionary describing all the selections"""
        if self._selections is not None:
            return self._selections
        if "all" in self.config.Selections:
            sel_names = RailSelectionFactory.get_selection_names()
        else:
            sel_names = self.config.Selections
        return {name_: RailSelectionFactory.get_selection(name_) for name_ in sel_names}

    def get_selection(self, name: str) -> RailSelection:
        """Get a particular selection by name"""
        selections = self.get_selections()
        try:
            return selections[name]
        except KeyError as missing_key:
            raise KeyError(
                f"Selection '{name}' not found in {self}. "
                f"Known values are {list(selections.keys())}"
            ) from missing_key

    def get_algorithms(self, algorithm_type: str) -> dict[str, dict[str, str]]:
        sub_algo_dict = self._algorithms.get(algorithm_type, {})
        if sub_algo_dict:
            return sub_algo_dict
        algo_names = self.config[algorithm_type]
        all_algos = RailAlgorithmFactory.get_algorithms(algorithm_type)
        if "all" in algo_names:
            use_algos = list(all_algos.values())
        else:
            use_algos = [all_algos[key] for key in algo_names]
        for algo_ in use_algos:
            algo_.fill_dict(sub_algo_dict)
        self._algorithms[algorithm_type] = sub_algo_dict
        return sub_algo_dict

    def get_algorithm(self, algorithm_type: str, algo_name: str) -> dict[str, str]:
        algo_dict = self.get_algorithms(algorithm_type)
        try:
            return algo_dict[algo_name]
        except KeyError as missing_key:
            raise KeyError(
                f"Algorithm '{algo_name}' of type '{algorithm_type}' not found in {self}. "
                f"Known values are {list(algo_dict.keys())}"
            ) from missing_key

    def get_error_models(self) -> dict:
        """Get the dictionary describing all the photometric error model algorithms"""
        return self.get_algorithms("ErrorModels")

    def get_error_model(self, name: str) -> dict:
        """Get the information about a particular photometric error model algorithms"""
        return self.get_algorithm("ErrorModels", name)

    def get_pzalgorithms(self) -> dict:
        """Get the dictionary describing all the PZ estimation algorithms"""
        return self.get_algorithms("PZAlgorithms")

    def get_pzalgorithm(self, name: str) -> dict:
        """Get the information about a particular PZ estimation algorithm"""
        return self.get_algorithm("PZAlgorithms", name)

    def get_spec_selections(self) -> dict:
        """Get the dictionary describing all the spectroscopic selection algorithms"""
        return self.get_algorithms("SpecSelections")

    def get_spec_selection(self, name: str) -> dict:
        """Get the information about a particular spectroscopic selection algorithm"""
        return self.get_algorithm("SpecSelections", name)

    def get_classifiers(self) -> dict:
        """Get the dictionary describing all the tomographic bin classification"""
        return self.get_algorithms("Classifiers")

    def get_classifier(self, name: str) -> dict:
        """Get the information about a particular tomographic bin classification"""
        return self.get_algorithm("Classifiers", name)

    def get_summarizers(self) -> dict:
        """Get the dictionary describing all the NZ summarization algorithms"""
        return self.get_algorithms("Summarizers")

    def get_summarizer(self, name: str) -> dict:
        """Get the information about a particular NZ summarization algorithms"""
        return self.get_algorithm("Summarizers", name)

    def get_catalogs(self) -> dict:
        """Get the dictionary describing all the types of data catalogs"""
        if self._catalog_templates is not None:
            return self._catalog_templates
        if "all" in self.config.Catalogs:
            self._catalog_templates = RailCatalogFactory.get_catalog_templates()
        else:
            self._catalog_templates = {
                key: RailCatalogFactory.get_catalog_template(key)
                for key in self.config.Catalogs
            }
        return self._catalog_templates

    def get_catalog_files(self, name: str, **kwargs: Any) -> list[str]:
        """Resolve the paths for a particular catalog file"""
        catalogs = self.get_catalogs()
        try:
            catalog = catalogs[name]
        except KeyError as missing_key:
            raise KeyError(
                f"catalogs '{name}' not found in {list(catalogs.keys())}"
            ) from missing_key
        interpolants = kwargs.copy()
        interpolants.update(**self.name_factory.get_resolved_common_paths())
        catalog_instance = catalog.make_catalog_instance("dummy", **interpolants)
        return catalog_instance(**self.config.IterationVars)

    def get_catalog(self, name: str, **kwargs: Any) -> str:
        """Resolve the path for a particular catalog file"""
        catalogs = self.get_catalogs()
        try:
            catalog = catalogs[name]
        except KeyError as missing_key:
            raise KeyError(
                f"catalogs '{name}' not found in {list(catalogs.keys())}"
            ) from missing_key

        try:
            path = self.name_factory.resolve_path(
                catalog.config.to_dict(), "path_template", **kwargs
            )
            return path
        except KeyError as msg:
            raise KeyError(f"path_template not found in {catalog}") from msg

    def get_pipelines(self) -> dict[str, RailPipelineTemplate]:
        """Get the dictionary describing all the types of ceci pipelines"""
        if self._pipeline_templates is not None:
            return self._pipeline_templates
        if "all" in self.config.Pipelines:
            self._pipeline_templates = RailPipelineFactory.get_pipeline_templates()
        else:
            self._pipeline_templates = {
                key: RailPipelineFactory.get_pipeline_template(key)
                for key in self.config.Pipelines
            }
        return self._pipeline_templates

    def get_pipeline(self, name: str) -> RailPipelineTemplate:
        """Get the information about a particular ceci pipeline"""
        pipelines = self.get_pipelines()
        try:
            return pipelines[name]
        except KeyError as missing_key:
            raise KeyError(
                f"pipeline '{name}' not found in {list(pipelines.keys())}"
            ) from missing_key

    def get_flavor_args(self, flavors: list[str]) -> list[str]:
        """Get the 'flavors' to iterate a particular command over

        Notes
        -----
        If the flavor 'all' is included in the list of flavors, this
        will replace the list with all the flavors defined in this project
        """
        flavor_dict = self.get_flavors()
        if "all" in flavors:
            return list(flavor_dict.keys())
        return flavors

    def get_selection_args(self, selections: list[str]) -> list[str]:
        """Get the 'selections' to iterate a particular command over

        Notes
        -----
        If the selection 'all' is included in the list of selections, this
        will replace the list with all the selections defined in this project
        """
        selection_dict = self.get_selections()
        if "all" in selections:
            return list(selection_dict.keys())
        return selections

    def generate_kwargs_iterable(self, **iteration_dict: Any) -> list[dict]:
        iteration_vars = list(iteration_dict.keys())
        iterations = itertools.product(
            *[iteration_dict.get(key, []) for key in iteration_vars]
        )
        iteration_kwarg_list = []
        for iteration_args in iterations:
            iteration_kwargs = {
                iteration_vars[i]: iteration_args[i] for i in range(len(iteration_vars))
            }
            iteration_kwarg_list.append(iteration_kwargs)
        return iteration_kwarg_list

    def generate_ceci_command(
        self,
        pipeline_path: str,
        config: str | None,
        inputs: dict,
        output_dir: str = ".",
        log_dir: str = ".",
        **kwargs: Any,
    ) -> list[str]:
        if config is None:
            config = pipeline_path.replace(".yaml", "_config.yml")

        command_line = [
            "ceci",
            f"{pipeline_path}",
            f"config={config}",
            f"output_dir={output_dir}",
            f"log_dir={log_dir}",
        ]

        for key, val in inputs.items():
            command_line.append(f"inputs.{key}={val}")

        for key, val in kwargs.items():
            command_line.append(f"{key}={val}")

        return command_line

    def build_pipelines(
        self,
        flavor: str = "baseline",
        *,
        force: bool = False,
    ) -> int:
        """Build ceci pipeline configuraiton files for this project

        Parameters
        ----------
        flavor: str
            Which analysis flavor to draw from

        force: bool
            Force overwriting of existing pipeline files

        Returns
        -------
        status_code: int
            0 if ok, error code otherwise
        """
        output_dir = self.get_common_path("project_scratch_dir")
        flavor_dict = self.get_flavor(flavor)
        pipelines_to_build = flavor_dict["pipelines"]
        pipeline_overrides = flavor_dict["pipeline_overrides"]
        do_all = "all" in pipelines_to_build

        for pipeline_name, pipeline_info in self.get_pipelines().items():
            if not (do_all or pipeline_name in pipelines_to_build):
                print(f"Skipping pipeline {pipeline_name} from flavor {flavor}")
                continue
            output_yaml = self.get_path(
                "pipeline_path", pipeline=pipeline_name, flavor=flavor
            )
            if os.path.exists(output_yaml):  # pragma: no cover
                if force:
                    print(f"Overwriting existing pipeline {output_yaml}")
                else:
                    print(f"Skipping existing pipeline {output_yaml}")
                    continue
            pipe_out_dir = os.path.dirname(output_yaml)

            try:
                os.makedirs(pipe_out_dir)
            except FileExistsError:
                pass

            overrides = pipeline_overrides.get("default", {})
            overrides.update(**pipeline_overrides.get(pipeline_name, {}))

            pipeline_kwargs = pipeline_info["kwargs"]
            for key, val in pipeline_kwargs.items():
                if isinstance(val, list) and "all" in val:
                    if key == "selectors":
                        pipeline_kwargs[key] = self.get_spec_selections()
                    elif key == "algorithms":
                        pipeline_kwargs[key] = self.get_pzalgorithms()
                    elif key == "classifiers":
                        pipeline_kwargs[key] = self.get_classifiers()
                    elif key == "summarizers":
                        pipeline_kwargs[key] = self.get_summarizers()
                    elif key == "error_models":
                        pipeline_kwargs[key] = self.get_error_models()

            if overrides:
                pipe_ctor_kwargs = overrides.pop("kwargs", {})
                pz_algorithms = pipe_ctor_kwargs.pop("PZAlgorithms", None)

                if pz_algorithms:
                    orig_pz_algorithms = self.get_pzalgorithms().copy()
                    pipe_ctor_kwargs["algorithms"] = {
                        pz_algo_: orig_pz_algorithms[pz_algo_]
                        for pz_algo_ in pz_algorithms
                    }
                    pipeline_kwargs.update(**pipe_ctor_kwargs)

                stages_config = os.path.join(
                    pipe_out_dir, f"{pipeline_name}_{flavor}_overrides.yml"
                )
                with open(stages_config, "w", encoding="utf-8") as fout:
                    yaml.dump(overrides, fout)
            else:
                stages_config = None

            pipeline_class = pipeline_info["pipeline_class"]
            catalog_tag = flavor_dict["catalog_tag"]

            if catalog_tag:
                catalog_utils.apply_defaults(catalog_tag)

            tokens = pipeline_class.split(".")
            module = ".".join(tokens[:-1])
            class_name = tokens[-1]
            log_dir = f"{output_dir}/logs/{pipeline_name}"

            print(f"Writing {output_yaml}")

            __import__(module)
            RailPipeline.build_and_write(
                class_name,
                output_yaml,
                None,
                stages_config,
                output_dir,
                log_dir,
                **pipeline_kwargs,
            )

        return 0

    def subsample_data(
        self,
        catalog_template: str,
        file_template: str,
        subsampler_class_name: str,
        subsample_name: str,
        dry_run: bool = False,
        **kwargs: dict[str, Any],
    ) -> str:
        """Subsammple some data

        Parameters
        ----------
        catalog_template: str
            Tag for the input catalog

        file_template: str
            Which label to apply to output dataset

        subsampler_class_name: str,
            Name of the class to use for subsampling

        subsample_name: str,
            Name of the subsample to create

        dry_run: bool
            If true, do not actually run

        Keywords
        --------
        Used to provide values for additional interpolants, e.g., flavor, basename, etc...

        Returns
        -------
        output_path: str
            Path to output file
        """
        hdf5_output = self.get_file(file_template, **kwargs)
        output = hdf5_output.replace(".hdf5", ".parquet")

        subsampler_class = library.get_algorithm_class(
            "Subsamplers", subsampler_class_name, "Subsample"
        )
        subsampler_args = library.get_subsample(subsample_name)
        subsampler = subsampler_class(**subsampler_args.config.to_dict())

        sources = self.get_catalog_files(catalog_template, **kwargs)

        # output_dir = os.path.dirname(output)
        if not dry_run:
            subsampler(sources, output)

        return output

    def reduce_data(
        self,
        catalog_template: str,
        output_catalog_template: str,
        reducer_class_name: str,
        input_selection: str,
        selection: str,
        dry_run: bool = False,
        **kwargs: Any,
    ) -> list[str]:
        """Reduce some data

        Parameters
        ----------
        catalog_template: str
            Tag for the input catalog

        output_catalog_template: str
            Which label to apply to output dataset

        reducer_class_name: str,
            Name of the class to use for subsampling

        input_selection: str,
            Selection to use for the input

        selection: str,
            Selection to apply

        dry_run: bool
            If true, do not actually run

        Keywords
        --------
        Used to provide values for additional interpolants, e.g.,

        Returns
        -------
        sinks: list[str]
            Paths to output files
        """
        sources = self.get_catalog_files(catalog_template, selection=input_selection, **kwargs)        
        sinks = self.get_catalog_files(output_catalog_template, selection=selection, **kwargs)

        reducer_class = library.get_algorithm_class(
            "Reducers", reducer_class_name, "Reduce"
        )
        reducer_args = library.get_selection(selection)
        reducer = reducer_class(**reducer_args.config.to_dict())

        # output_dir = os.path.dirname(output)
        if not dry_run:
            reducer(sources, sinks)

        return sinks
