from __future__ import annotations

from typing import Any
import os
import yaml


from ceci.config import StageParameter
from .configurable import Configurable
from .factory_mixin import RailFactoryMixin


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
    yaml_tag = "Selection"

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


class RailSelectionFactory(RailFactoryMixin):
    """Factory class to make selections

    Expected usage is that user will define a yaml file with the various
    datasets that they wish to use with the following example syntax:
    Selections:
      - Selection:
          name: maglim_25.5
          cuts:
                maglim_i: [null, 25.5]
    """

    client_classes = [RailSelection]

    _instance: RailSelectionFactory | None = None

    def __init__(self) -> None:
        """C'tor, build an empty RailDatasetFactory"""
        RailFactoryMixin.__init__(self)
        self._selections = self.add_dict(RailSelection)

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

    def print_instance_contents(self) -> None:
        """Print the contents of the factory"""
        print("----------------")
        print("Selections:")
        for selection_name, selection in self.selections.items():
            print(f"  {selection_name}: {selection}")

    def add_selection(self, selection: RailSelection) -> None:
        self.add_to_dict(selection)

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
        self.load_instance_yaml_tag(selections_config)

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
