"""Executable arithmetic specification for RESEARCH_PROTOCOL.md §6.

No production cohort/scoring pipeline is introduced here. These fixtures pin
the corrected bound to independently enumerated coefficient-box maxima so the
future implementation cannot reintroduce pooling or estimator-weight errors.
When the production bound exists, add it to BOUND_IMPLEMENTATIONS so every
fixture runs against it as well as the reference arithmetic.
"""
from itertools import product

import numpy as np
import pytest


def grouped_bound(groups, weights, side, payout, basis, margins):
    """Reference arithmetic for already validated inputs and assigned fits."""
    exposure = weights[:, None] * side[:, None] * payout[:, None] * basis
    return sum(np.abs(exposure[groups == g].sum(axis=0)) @ margins[g]
               for g in range(len(margins)))


# Signature: (groups, weights, side, payout, basis, margins) -> float.
BOUND_IMPLEMENTATIONS = [grouped_bound]
bound_impl = pytest.mark.parametrize("bound_fn", BOUND_IMPLEMENTATIONS,
                                     ids=lambda f: f.__name__)


@bound_impl
def test_opposite_sides_at_different_offsets_cannot_cancel_the_bound(bound_fn):
    groups = np.array([0, 1])
    weights = np.array([0.5, 0.5])
    side = np.array([1, -1])
    payout = np.array([2., 2.])
    # p=0.5, mean(p-0.5)=0 and mean(((p-0.5)/r)^2)=1/3 in each fit.
    basis = np.array([[1., 0., -1/3], [1., 0., -1/3]])
    margins = np.array([[.005, .05, .016], [.005, .05, .016]])
    pooled = np.abs((weights[:, None] * side[:, None] * payout[:, None] * basis)
                    .sum(axis=0)) @ margins[0]
    allowed_intercepts = np.array([.004, -.004])
    actual = np.sum(weights * side * payout * allowed_intercepts)
    bound = bound_fn(groups, weights, side, payout, basis, margins)
    assert pooled == 0
    assert actual == pytest.approx(.008)
    assert bound == pytest.approx(.010 + .016 * 2/3)
    assert bound > actual > pooled


@bound_impl
def test_grouped_bound_matches_exhaustive_coefficient_box_maximum(bound_fn):
    groups = np.array([0, 0, 1, 1, 1])
    weights = np.array([.25, .25, 1/6, 1/6, 1/6])
    side = np.array([1, -1, 1, -1, 1])
    payout = np.array([2., 3., 1.5, 2.5, 4.])
    basis = np.array([[1, -.1, .3], [1, .2, -.2], [1, -.2, .4],
                      [1, .15, -.1], [1, 0., -.3]])
    margins = np.array([[.005, .05, .016], [.004, .04, .012]])
    bound = bound_fn(groups, weights, side, payout, basis, margins)
    values = []
    for signs in product((-1, 1), repeat=6):
        coefficients = margins * np.array(signs).reshape(2, 3)
        drift = np.sum(basis * coefficients[groups], axis=1)
        values.append(np.sum(weights * side * payout * drift))
    assert bound == pytest.approx(max(values))
    assert -bound == pytest.approx(min(values))


@bound_impl
def test_event_weights_preserve_bound_when_an_entry_is_duplicated_within_event(bound_fn):
    margins = np.array([[.005, .05, .016]])
    basis = np.array([[1., 0., -.3], [1., 0., -.3]])
    base = bound_fn(np.array([0, 0]), np.array([.5, .5]), np.array([1, -1]),
                         np.array([2., 2.]), basis, margins)
    # Event B's identical entry is duplicated; each gets half of B's weight.
    idx = np.array([0, 1, 1])
    event_weighted = bound_fn(np.zeros(3, dtype=int), np.array([.5, .25, .25]),
                                   np.array([1, -1, -1]), np.full(3, 2.),
                                   basis[idx], margins)
    entry_weighted = bound_fn(np.zeros(3, dtype=int), np.full(3, 1/3),
                                   np.array([1, -1, -1]), np.full(3, 2.),
                                   basis[idx], margins)
    assert event_weighted == base == 0  # cancellation is valid within one fit
    assert entry_weighted > event_weighted
