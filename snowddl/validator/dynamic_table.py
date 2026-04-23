from snowddl.blueprint import DynamicTableBlueprint
from snowddl.validator.abc_validator import AbstractValidator


class DynamicTableValidator(AbstractValidator):
    def get_blueprints(self):
        return self.config.get_blueprints_by_type(DynamicTableBlueprint)

    def validate_blueprint(self, bp: DynamicTableBlueprint):
        pass
