from app.db.Models.flow_context import TargetField


class DTOFields():

    @staticmethod
    def from_dto_dict_to_dao_dict(d):
        new_d = {**d, 'rules':[]}

        for rule in d.get('rules', []):
            if "property" in rule and rule["property"]:
                rule["property"] = rule["property"]['value']

            new_d['rules'].append(rule)

        return d

    @staticmethod
    def from_dao_to_dto(dao: TargetField, domain_id):
        dto = DTOFields()
        dto.__dict__ = {**dao.__dict__, "rules":[]}
        dto.id = dao.id

        dto.rules = []
        for rule in dao.rules:
            if "property" in rule and rule["property"]:
                tf = TargetField(id=rule["property"]).load(domain_id=domain_id)
                rule["property"] = {'value': tf.id, 'label':tf.label}

            dto.rules.append(rule)

        return dto
