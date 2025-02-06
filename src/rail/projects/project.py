from __future__ import annotations

import itertools
import os
from typing import Any

import yaml
from ceci.config import StageParameter

from . import execution, library, name_utils
from .algorithm_factory import RailAlgorithmFactory
from .catalog_factory import RailCatalogFactory
from .catalog_template import RailProjectCatalogTemplate
from .configurable import Configurable
from .file_template import RailProjectFileTemplate
from .pipeline_factory import RailPipelineFactory
from .pipeline_holder import RailPipelineTemplate
from .project_file_factory import RailProjectFileFactory
from .reducer import RailReducer
from .selection_factory import RailSelection, RailSelectionFactory
from .subsample_factory import RailSubsample, RailSubsampleFactory


class RailFlavor(Configurable):
    """Description of a single analysis variation

    This includes

    a name for the variant, used to construct filenames

    a 'catalog_tag', which identifies format of the data being used, and sets
    the expected names of columns accordingly

    a list of 'pipelines' that can be run in this variant

    a list of 'file_aliases' that can be used to specify the
    input files used in this variant

    a dict of 'pipeline_overrides' that modify the behavior of the various pipelines
    """

    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Flavor name"),
        catalog_tag=StageParameter(
            str, None, fmt="%s", required=False, msg="tag for catalog being used"
        ),
        pipelines=StageParameter(list, ["all"], fmt="%s", msg="pipelines being used"),
        file_aliases=StageParameter(dict, {}, fmt="%s", msg="file aliases used"),
        pipeline_overrides=StageParameter(dict, {}, fmt="%s", msg="file aliases used"),
    )

    def __init__(self, **kwargs: Any) -> None:
        """C'tor

        Parameters
        ----------
        **kwargs:
            Configuration parameters for this RailFlavor, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)


class RailProject(Configurable):
    """Main analysis driver class, this collects all the elements needed to
    run a collection of studies using RAIL.

    The key concepts are:

    1. analysis 'Flavors', which are versions of
    similar analyses with slightly different parameter settings and/or
    input files.

    2. ceci 'Pipelines', which run blocks of analysis code

    A RailProject basically specifies which Pipelines to run under which
    flavors, and keeps track of the outputs.

    RailProject.functionality_help() for more on class functionality

    RailProject.configuration_help() for more on class configuration
    """

    config_options: dict[str, StageParameter] = dict(
        Name=StageParameter(str, None, fmt="%s", required=True, msg="Project name"),
        Includes=StageParameter(list, [], fmt="%s", msg="Files to include"),
        Baseline=StageParameter(
            dict, None, fmt="%s", required=True, msg="Baseline analysis configuration"
        ),
        Flavors=StageParameter(list, [], fmt="%s", msg="Analysis variants"),
        PathTemplates=StageParameter(dict, {}, fmt="%s", msg="File path templates"),
        CommonPaths=StageParameter(
            dict, {}, fmt="%s", required=True, msg="Paths to shared directories"
        ),
        IterationVars=StageParameter(
            dict, {}, fmt="%s", msg="Iteration variables to use"
        ),
        Selections=StageParameter(
            list, ["all"], fmt="%s", msg="Data selections to use"
        ),
        Subsamples=StageParameter(
            list, ["all"], fmt="%s", msg="Subsample defintions to use"
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
    )

    yaml_tag: str = "Project"

    projects: dict[str, RailProject] = {}

    @classmethod
    def functionality_help(cls) -> None:
        """
        The main functions that the use will use using are:

        load_config:
        ------------
        Read a yaml file and create a RailProject

        reduce_data:
        ------------
        Make a reduced catalog from an input catalog by applying a selction
        and trimming unwanted colums.  This is run before the analysis pipelines.

        subsample_data:
        ---------------
        Subsample data from a catalog to make a testing or training file.
        This is run after catalog level pipelines, but before pipeliens run
        on indvidudal training/ testing samples

        build_pipelines:
        ----------------
        Build ceci pipeline yaml files

        run_pipeline_single:
        --------------------
        Run a pipeline on a single file

        run_pipeline_catalog:
        ---------------------
        Run a pipeline on a catalog of files
        """
        print(cls.functionality_help.__doc__)

    @classmethod
    def configuration_help(cls) -> None:
        """
        Configuring a RailProject

        Most of these element come from the shared library of elements,
        which is accesible from rail.projects.library

        ==========================
        Shared configuration files
        ==========================

        Includes: list[str]
        -------------------
        List of shared configuration files to load

        ========================
        Project analysis flavors
        ========================

        See :py:class:`rail.projects.project.RailFlavor` for the parameters
        needed to define an analysis 'Flavor'.

        Baseline: dict[str, Any]
        ------------------------
        Baseline configuration for this project.
        This is included in all the other analysis flavors

        Flavors: list[dict[str, Any]]
        -----------------------------
        List of all the analysis flavors that have been defined in this project

        ====================
        Bookkeeping elements
        ====================

        These are used to define the file paths for the project.

        PathTemplates: dict[str, str]
        -----------------------------
        Overrides for templates used to construct file paths

        The defaults are given in :py:mod:`rail.projects.name_utils`

        .. code-block:: python

            PathTemplates = dict(
                pipeline_path="{pipelines_dir}/{pipeline}_{flavor}.yaml",
                ceci_output_dir="{project_dir}/data/{selection}_{flavor}",
                ceci_file_path="{tag}_{stage}.{suffix}",
            )

        CommonPaths: dict[str, str]
        ---------------------------
        Defintions of common paths used to construct file paths

        The defaults are given in :py:mod:`rail.projects.name_utils`

        .. code-block:: python

            CommonPaths = dict(
                root=".",          # needs to be overridden
                scratch_root=".",  # needs to be overridden
                project="",        # needs to be overridden
                project_dir="{root}/projects/{project}",
                project_scratch_dir="{scratch_root}/projects/{project}",
                catalogs_dir="{root}/catalogs",
                pipelines_dir="{project_dir}/pipelines",
            )

        IterationVars: dict[str, list[str]]
        -----------------------------------
        Iteration variables to construct the catalogs


        ===============
        Shared elements
        ===============
        Things that are pulled from the library, each of these is just a list
        of the names of things that are defined in the library that
        can be used in this project.  The default is to use all the
        items defined in the library.

        Catalogs: list[str] These are actually CatalogTemplates

        Files: list[str] These are actually FileTemplates

        Pipelines: list[str] These are actually PipelineTemplates

        Reducers: list[str] These reduce the input data catalog

        Subsamplers: list[str] These subsample catalogs to get individual files

        Selections: list[str] These are the selection parameters

        Subsamples: list[str] These are the subsample parameters

        PZAlgorithms: list[str]

        SpecSelections: list[str]

        Classifiers: list[str]

        Summarizers: list[str]

        ErrorModels: list[str]
        """
        print(cls.configuration_help.__doc__)

    def __init__(self, **kwargs: Any) -> None:
        """C'tor

        Parameters
        ----------
        **kwargs:
            Configuration parameters for this RailProject, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

        for include_ in self.config.Includes:
            library.load_yaml(os.path.expandvars(include_))

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
        self._subsamples: dict[str, RailSubsample] | None = None
        self._flavors: dict[str, RailFlavor] | None = None

    def __repr__(self) -> str:
        return f"{self.config.Name}"

    def clear_cache(self) -> None:
        """Reset all the cached configurable items"""
        self._catalog_templates = None
        self._file_templates = None
        self._pipeline_templates = None
        self._algorithms = {}
        self._selections = None
        self._subsamples = None
        self._flavors = None

    @staticmethod
    def generate_kwargs_iterable(**iteration_dict: Any) -> list[dict]:
        """Generate a list of kwargs dicts from a dict of lists"""
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

    @staticmethod
    def generate_ceci_command(
        pipeline_path: str,
        config: str | None,
        inputs: dict,
        output_dir: str = ".",
        log_dir: str = ".",
        **kwargs: Any,
    ) -> list[str]:
        """Generate a ceci command to run a pipeline

        Parameters
        ----------
        pipeline_path: str
            Path to the pipline yaml file

        config: str | None
            Path to the pipeline config yaml file

        inputs: dict
            Input to the pipeline

        output_dir: str, default="."
            Pipeline output directory

        log_dir: str, default="."
            Pipeline log directory

        **kwargs:
            These are appended to the command in key=value pairs
        """

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

    @property
    def name(self) -> str:
        return self.config.Name

    @staticmethod
    def load_config(config_file: str) -> RailProject:
        """Create and return a RailProject from a yaml config file"""
        with open(os.path.expandvars(config_file), "r", encoding="utf-8") as fp:
            config_orig = yaml.safe_load(fp)

        project_config = config_orig.get("Project")
        project = RailProject(**project_config)
        RailProject.projects[project.name] = project
        return project

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

        **kwargs:
            Used to provide values for additional interpolants.

        Returns
        -------
        list[str]:
            Paths to output files

        """
        sources = self.get_catalog_files(
            catalog_template, selection=input_selection, **kwargs
        )
        sinks = self.get_catalog_files(
            output_catalog_template, selection=selection, **kwargs
        )

        reducer_class = library.get_algorithm_class(
            "Reducer", reducer_class_name, "Reduce"
        )
        assert issubclass(reducer_class, RailReducer)

        reducer_args = library.get_selection(selection)
        reducer = reducer_class(**reducer_args.config.to_dict())

        if not dry_run:  # pragma: no cover
            for source_, sink_ in zip(sources, sinks):
                reducer.run(source_, sink_)

        return sinks

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

        **kwargs:
            Used to provide values for additional interpolants, e.g., flavor, basename, etc...

        Returns
        -------
        str:
            Path to output file
        """
        hdf5_output = self.get_file(file_template, **kwargs)
        output = hdf5_output.replace(".hdf5", ".parquet")

        subsampler_class = library.get_algorithm_class(
            "Subsampler", subsampler_class_name, "Subsample"
        )
        subsampler_args = library.get_subsample(subsample_name)
        subsampler = subsampler_class(**subsampler_args.config.to_dict())

        sources = self.get_catalog_files(catalog_template, **kwargs)

        # output_dir = os.path.dirname(output)
        if not dry_run:  # pragma: no cover
            subsampler.run(sources, output)

        return output

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
        int:
            0 if ok, error code otherwise
        """
        flavor_dict = self.get_flavor(flavor)
        pipelines_to_build = flavor_dict["pipelines"]
        all_flavor_overrides = flavor_dict.get("pipeline_overrides", {}).copy()
        do_all = "all" in pipelines_to_build

        ok = 0
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

            overrides = all_flavor_overrides.get("default", {}).copy()
            pipeline_overrides = all_flavor_overrides.get(pipeline_name, {}).copy()
            overrides.update(**pipeline_overrides)
            pipeline_instance = pipeline_info.make_instance(self, flavor, overrides)
            ok |= pipeline_instance.build(self)

        return ok

    def make_pipeline_single_input_command(
        self,
        pipeline_name: str,
        flavor: str,
        **kwargs: Any,
    ) -> list[str]:
        """Build the command to run pipeline on a single file

        Parameters
        ----------
        pipeline_name: str
            Pipeline in question

        flavor: str
            Flavor to apply

        **kwargs: Any
            Other interpolants, such as selection

        Returns
        -------
        list[str]:
            Tokens in the command line, usable by subprocess.run()
        """
        pipeline_template = self.get_pipeline(pipeline_name)
        pipeline_instance = pipeline_template.make_instance(self, flavor, {})
        return pipeline_instance.make_pipeline_single_input_command(self, **kwargs)

    def make_pipeline_catalog_commands(
        self,
        pipeline_name: str,
        flavor: str,
        **kwargs: Any,
    ) -> list[tuple[list[list[str]], str]]:
        """Build the commands to run pipeline on a catalog

        Parameters
        ----------
        pipeline_name: str
            Pipeline in question

        flavor: str
            Flavor to apply

        **kwargs: Any
            Other interpolants, such as selection

        Returns
        -------
        list[tuple[list[list[str]], str]:
            List of pairs of series of commands and potential location for slurm batch file
        """
        pipeline_template = self.get_pipeline(pipeline_name)
        pipeline_instance = pipeline_template.make_instance(self, flavor, {})
        return pipeline_instance.make_pipeline_catalog_commands(self, **kwargs)

    def run_pipeline_single(
        self,
        pipeline_name: str,
        run_mode: execution.RunMode = execution.RunMode.bash,
        **kwargs: Any,
    ) -> int:
        """Run pipeline on a single file

        Parameters
        ----------
        pipeline_name: str
            Pipeline in question

        run_mode: execution.RunMode
            How to run the pipeline (e.g., in bash, or in slurm)

        **kwargs: Any
            Other interpolants, such as selection

        Returns
        -------
        int:
            0 for success, error code otherwise
        """
        kwcopy = kwargs.copy()
        flavor = kwcopy.pop("flavor")
        sink_dir = self.get_path("ceci_output_dir", flavor=flavor, **kwcopy)
        script_path = os.path.join(sink_dir, f"submit_{pipeline_name}.sh")
        commands = self.make_pipeline_single_input_command(
            pipeline_name, flavor, **kwcopy
        )
        try:
            statuscode = execution.handle_commands(run_mode, [commands], script_path)
        except Exception as msg:  # pragma: no cover
            print(msg)
            statuscode = 1
        return statuscode

    def run_pipeline_catalog(
        self,
        pipeline_name: str,
        run_mode: execution.RunMode = execution.RunMode.bash,
        **kwargs: Any,
    ) -> int:
        """Run pipeline on a catalog

        Parameters
        ----------
        pipeline_name: str
            Pipeline in question

        run_mode: execution.RunMode
            How to run the pipeline (e.g., in bash, or in slurm)

        **kwargs: Any
            Other interpolants, such as selection

        Returns
        -------
        int:
            0 for success, error code otherwise
        """

        kwcopy = kwargs.copy()
        flavor = kwcopy.pop("flavor")
        all_commands = self.make_pipeline_catalog_commands(
            pipeline_name, flavor, **kwcopy
        )

        ok = 0
        for commands, script_path in all_commands:
            try:
                execution.handle_commands(
                    run_mode,
                    commands,
                    script_path,
                )
            except Exception as msg:  # pragma: no cover
                print(msg)
                ok |= 1

        return ok

    def add_flavor(self, name: str, **kwargs: Any) -> RailFlavor:
        """Add a new flavor to the Project"""
        if self._flavors is None:  # pragma: no cover
            self.get_flavors()
        assert self._flavors is not None
        if name in self._flavors:
            raise KeyError(f"Flavor {name} already in RailProject {self.name}")
        flavor_params = self.config.Baseline.copy()
        flavor_params.update(name=name, **kwargs)
        new_flavor = RailFlavor(**flavor_params)
        self.config["Flavors"].append(new_flavor.config.to_dict())
        self._flavors[new_flavor.config.name] = new_flavor
        return new_flavor

    def write_yaml(self, yaml_file: str) -> None:
        """Write this project to a yaml file"""
        the_dict = self.to_yaml_dict()
        with open(os.path.expandvars(yaml_file), "w", encoding="utf-8") as fout:
            yaml.dump(the_dict, fout)

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
        else:  # pragma: no cover
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

    def get_file_metadata_for_flavor(
        self, flavor: str, label: str
    ) -> RailProjectFileTemplate:
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
        sel_names = (
            RailSelectionFactory.get_selection_names()
            if "all" in self.config.Selections
            else self.config.Selections
        )
        self._selections = {
            name_: RailSelectionFactory.get_selection(name_) for name_ in sel_names
        }
        return self._selections

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

    def get_subsamples(self) -> dict[str, RailSubsample]:
        """Get the dictionary describing all the subsamples"""
        if self._subsamples is not None:
            return self._subsamples
        sel_names = (
            RailSubsampleFactory.get_subsample_names()
            if "all" in self.config.Subsamples
            else self.config.Subsamples
        )
        self._subsamples = {
            name_: RailSubsampleFactory.get_subsample(name_) for name_ in sel_names
        }
        return self._subsamples

    def get_subsample(self, name: str) -> RailSubsample:
        """Get a particular subsample by name"""
        subsamples = self.get_subsamples()
        try:
            return subsamples[name]
        except KeyError as missing_key:
            raise KeyError(
                f"Subsample '{name}' not found in {self}. "
                f"Known values are {list(subsamples.keys())}"
            ) from missing_key

    def get_algorithms(self, algorithm_type: str) -> dict[str, dict[str, str]]:
        """Get all the algorithms of a particular type"""
        sub_algo_dict = self._algorithms.get(algorithm_type, {})
        if sub_algo_dict:
            return sub_algo_dict
        algo_names = self.config[algorithm_type]
        # trim off the trailing 's'
        all_algos = RailAlgorithmFactory.get_algorithms(algorithm_type[0:-1])
        use_algos = (
            list(all_algos.values())
            if "all" in algo_names
            else [all_algos[key] for key in algo_names]
        )
        for algo_ in use_algos:
            algo_.fill_dict(sub_algo_dict)
        self._algorithms[algorithm_type] = sub_algo_dict
        return sub_algo_dict

    def get_algorithm(self, algorithm_type: str, algo_name: str) -> dict[str, str]:
        """Get an algorithm of a particular type with a specific name"""
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

        self._catalog_templates = (
            RailCatalogFactory.get_catalog_templates()
            if "all" in self.config.Catalogs
            else {
                key: RailCatalogFactory.get_catalog_template(key)
                for key in self.config.Catalogs
            }
        )
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
        return catalog_instance.resolve(**self.config.IterationVars)

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
        except KeyError as missing_key:
            raise KeyError(f"path_template not found in {catalog}") from missing_key

    def get_pipelines(self) -> dict[str, RailPipelineTemplate]:
        """Get the dictionary describing all the types of ceci pipelines"""
        if self._pipeline_templates is not None:
            return self._pipeline_templates
        self._pipeline_templates = (
            RailPipelineFactory.get_pipeline_templates()
            if "all" in self.config.Pipelines
            else {
                key: RailPipelineFactory.get_pipeline_template(key)
                for key in self.config.Pipelines
            }
        )
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
