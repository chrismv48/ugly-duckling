# Standard library imports
import json
import datetime

# Third-party imports
import arrow


class SerializedModel(object):
    """A SQLAlchemy model mixin class that can serialize itself as JSON."""

    @property
    def table_fields(self):
        return [key for key in self.__table__.columns.keys() if not key.startswith('_')]

    def as_dict(self):
        """Return dict representation of class by iterating over database
        columns."""
        value = {}
        for column in self.__table__.columns:
            attribute = getattr(self, column.name)
            if isinstance(attribute, datetime.datetime):
                attribute = arrow.get(attribute, 'US/Eastern').isoformat()
            value[column.name] = attribute
        return value

    def from_dict(self, attributes):
        """Update the current instance based on attribute->value items in
        *attributes*."""
        for attribute in attributes:
            if attribute in self.table_fields:
                setattr(self, attribute, attributes[attribute])
        return self

    def as_json(self):
        """Return JSON representation taken from dict representation."""
        return json.dumps(self.as_dict())
