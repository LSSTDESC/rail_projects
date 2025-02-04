from __future__ import annotations

from typing import Any

from ceci.config import StageParameter

from .configurable import Configurable
from .factory_mixin import RailFactoryMixin


class RailSelection(Configurable):
    """Paramters for a simple data selection

    This is just defined as a dict of cuts
    """

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
        **kwargs
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

    .. highlight:: yaml
    .. code-block:: yaml

      Selections:
        - Selection:
            name: maglim_25.5
            cuts:
              maglim_i: [null, 25.5]
    """

    yaml_tag = "Selections"

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
        RailSelection
            selection in question
        """
        try:
            return cls.instance().selections[name]
        except KeyError as msg:
            raise KeyError(
                f"RailSelection named {name} not found in RailSelectionFactory "
                f"{list(cls.instance().selections.keys())}"
            ) from msg

    @classmethod
    def add_selection(cls, selection: RailSelection) -> None:
        cls.instance().add_to_dict(selection)

    @property
    def selections(self) -> dict[str, RailSelection]:
        """Return the dictionary of selection templates"""
        return self._selections
