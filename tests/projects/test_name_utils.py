import pytest

from rail.projects import name_utils


def test_name_utils() -> None:
    assert name_utils.get_required_interpolants("xx_{alice}_{bob}") == [
        "{alice}",
        "{bob}",
    ]
    assert (
        name_utils.format_template("xx_{alice}_{bob}", alice="x", bob="x") == "xx_x_x"
    )

    test_dict = dict(
        a="a_{alice}",
        # b=['b1_{alice}', 'b2_{bob}'],
        c=dict(c1="c1_{alice}", c2="c2_{alice}"),
        # c2=dict(c2_1='c1_{alice}', c2_2=['c2_{alice}', 'c2_{bob}']),
    )

    name_utils.resolve_dict(test_dict, dict(alice="x", bob="y"))

    assert not name_utils.resolve_dict(None, {})
    with pytest.raises(ValueError):
        name_utils.resolve_dict(
            dict(
                a=(
                    "s",
                    "d",
                )
            ),
            dict(alice="x", bob="y"),
        )


def test_name_factory() -> None:
    null_factory = name_utils.NameFactory()

    with pytest.raises(KeyError):
        null_factory.get_template("nope", "nope")

    simple_factory = name_utils.NameFactory(
        templates=dict(
            alice="{bob}_{bob}",
        ),
    )

    assert simple_factory.resolve_template("CommonPaths", "root") == "."

    simple_factory.interpolants = dict(root="xx")
    assert simple_factory.interpolants["root"] == "xx"
    del simple_factory.interpolants
    assert simple_factory.interpolants.get("root") is None
