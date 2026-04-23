from snowddl.blueprint import FunctionBlueprint
from snowddl.validator.abc_validator import AbstractValidator


class FunctionValidator(AbstractValidator):
    def get_blueprints(self):
        return self.config.get_blueprints_by_type(FunctionBlueprint)

    def validate_blueprint(self, bp: FunctionBlueprint):
        pass
