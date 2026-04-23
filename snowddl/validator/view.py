from snowddl.blueprint import ViewBlueprint
from snowddl.validator.abc_validator import AbstractValidator


class ViewValidator(AbstractValidator):
    def get_blueprints(self):
        return self.config.get_blueprints_by_type(ViewBlueprint)

    def validate_blueprint(self, bp: ViewBlueprint):
        pass
