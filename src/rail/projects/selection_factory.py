from __future__ import annotations

from typing import Any
import os
import yaml


from ceci.config import StageParameter
from .configurable import Configurable


class RailSelection(Configurable):
    config_options: dict[str, StageParameter] = dict(
        name=StageParameter(str, None, fmt="%s", required=True, msg="Selection name"),
        cuts=StageParameter(
            dict,
            {},
            fmt="%s",
            msg="Cuts associated to selection",
        ),
    )

    def __init__(self, **kwargs: Any):
        """C'tor

        Parameters
        ----------
        kwargs: Any
            Configuration parameters for this RailAlgorithmHolder, must match
            class.config_options data members
        """
        Configurable.__init__(self, **kwargs)

    def __repr__(self) -> str:
        return f"cuts={self.config.cuts}"
        

class RailSelectionFactory:
    """Factory class to make selections

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:
    Selections:
      - Selection:
          name: maglim_25.5
          cuts:
                maglim_i: [null, 25.5]
    """

    _instance: RailSelectionFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        self._selections: dict[str, RailSelection] = {}

    @classmethod
    def instance(cls) -> RailSelectionFactory:
        """Return the singleton instance of the factory"""
        if cls._instance is None:
            cls._instance = RailSelectionFactory()
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
            cls._instance = RailSelectionFactory()
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
            cls._instance = RailSelectionFactory()
        cls._instance.load_instance_yaml(yaml_file)

    @classmethod
    def load_yaml_tag(cls, selections_config: list[dict[str, Any]]) -> None:
        """Load from a yaml tag

        Parameters
        ----------
        selections_config: list[dict[str, Any]]
            Yaml tag used to load

        Notes
        -----
        See class helpstring for yaml format
        """
        if cls._instance is None:
            cls._instance = RailSelectionFactory()
        cls._instance.load_selections_from_yaml_tag(selections_config)

    @classmethod
    def get_selections(cls) -> dict[str, RailSelection]:
        """Return the dict of all the selections"""
        return cls.instance().selections

    @classmethod
    def get_selection_names(cls) -> list[str]:
        """Return the names of the selections"""
        return list(cls.instance().selections.keys())

    @classmethod
    def get_selection(cls, name: str) -> RailSelection:
        """Get a selection by it's assigned name

        Parameters
        ----------
        name: str
            Name of the selection templates to return

        Returns
        -------
        selection: RailSelection
            selection  in question
        """
        try:
            return cls.instance().selections[name]
        except KeyError as msg:
            raise KeyError(
                f"RailSelection named {name} not found in RailSelectionFactory "
                f"{list(cls.instance().selections.keys())}"
            ) from msg

    @property
    def selections(self) -> dict[str, RailSelection]:
        """Return the dictionary of selection templates"""
        return self._selections

    def clear_instance(self) -> None:
        """Clear out the contents of the factory"""
        self._selections.clear()

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("----------------")
        print("Selections:")
        for selection_name, selection in self.selections.items():
            print(f"  {selection_name}: {selection}")

    def _make_selection(self, **kwargs: Any) -> RailSelection:
        try:
            name = kwargs["name"]
        except KeyError as missing_key:
            raise KeyError(
                "SelectionInstance yaml block does not contain name for selection_instance: "
                f"{list(kwargs.keys())}"
            ) from missing_key
        if name in self._selections:  # pragma: no cover
            raise KeyError(f"RailSelection {name} is already defined")
        selection = RailSelection(**kwargs)
        self._selections[name] = selection
        return selection

    def load_selection_from_yaml_tag(self, selection_config: dict[str, Any]) -> None:
        """Load a dataset from a Selection tag in yaml

        Paramters
        ---------
        selection_config: dict[str, Any]
            Yaml data in question
        """
        self._make_selection(**selection_config)

    def load_selections_from_yaml_tag(
        self,
        selections_config: list[dict[str, Any]],
    ) -> None:
        """Read a yaml "Selections" tag and load the factory accordingy

        Parameters
        ----------
        selections_config: list[dict[str, Any]]
            Yaml tag to load

        Notes
        -----
        See class description for yaml file syntax
        """
        for selections_item in selections_config:
            if "Selection" in selections_item:
                selection_config = selections_item["Selection"]
                self.load_selection_from_yaml_tag(selection_config)
            else:  # pragma: no cover
                good_keys = ["Selection"]
                raise KeyError(
                    f"Expecting one of {good_keys} not: {selections_item.keys()})"
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
            selections_config = yaml_data["Selections"]
        except KeyError as missing_key:
            raise KeyError(
                f"Did not find key Selections in {yaml_file}"
            ) from missing_key

        self.load_selections_from_yaml_tag(selections_config)
