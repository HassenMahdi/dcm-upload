from app.db.Models.User import User


def get_a_user(public_id):
    user = User().load({'_id': public_id})
    return user if user.id else None