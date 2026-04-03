"""Default validator implementation for MoleculerPy.

Wraps the existing validate_params / validate_param_rule logic from
``moleculerpy.validator`` and exposes it through the BaseValidator interface.
"""

from typing import Any

from moleculerpy.validator import validate_params

from .base import BaseValidator, CheckFunction


class DefaultValidator(BaseValidator):
    """Built-in validator based on the existing moleculerpy.validator module.

    Supports: string, number/int/float, boolean, array/list, object/dict, any,
    null, enum, required, optional, min, max, minLength, maxLength, pattern,
    minItems, maxItems, items, gt, gte, lt, lte.

    compile() caches a checker closure per schema so repeated calls are cheap.
    validate() delegates to compile() and executes the checker.
    """

    def compile(self, schema: dict[str, Any]) -> CheckFunction:
        """Compile schema into a cached checker function.

        Args:
            schema: Validation schema dict (maps param names to rules).

        Returns:
            A callable(params) that returns True or raises ValidationError.
        """

        def checker(params: dict[str, Any]) -> bool:
            return validate_params(params, schema)

        return checker

    def validate(self, params: dict[str, Any], schema: dict[str, Any]) -> bool:
        """Validate params against schema, raising ValidationError on failure.

        Args:
            params: Parameters to validate.
            schema: Validation schema dict.

        Returns:
            True if validation passes.

        Raises:
            ValidationError: If any validation rule fails.
        """
        checker = self.compile(schema)
        return checker(params)


__all__ = ["DefaultValidator"]
