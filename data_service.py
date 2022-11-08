import logging
from abc import ABC, abstractmethod
from typing import List, Mapping, Tuple, Type
from uuid import UUID

from model import Config, Job, User
from model_database import ModelDatabase, ModelDatabaseType, T
from utils import first_or_none

log = logging.getLogger(__name__)



def _default_password_encrypt(password: str) -> Tuple[str, str]:
    return password, "NO_SALT"

def _default_password_validate(password: str, password_hash: str, password_salt: str) -> bool:
    return password == password_hash



class DataServiceBase(ABC):
    @property
    @abstractmethod
    def _db(self): raise NotImplementedError

    @staticmethod
    def delete_check_not_exist(item, item_existing) -> bool:
        if item_existing is not None: return False
        if log.isEnabledFor(logging.DEBUG): log.debug(f"Attempt to delete non-existant {item}")
        log.warning(f"Attempt to delete non-existant {item}")
        return True


class DataServiceConfig(DataServiceBase, ABC):
    def save_config(self, config: Config):
        config.check()
        db = self._db[Job]
        db.save(config)

    def delete_config(self, config: Config):
        config.check()
        db = self._db[Config]
        config_existing = db.get_by_id(config.id)
        if self.delete_check_not_exist(config, config_existing): return
        db.delete(config)
        # TODO: check and cleanup unused configs

    def get_configs(self) -> Mapping[UUID, Config]:
        db = self._db[Config]
        return db.get_all()


class DataServiceUser(DataServiceBase, ABC):

    @staticmethod
    def save_user_check_system_user(user: User, users: Mapping[UUID, User]):
        user_existing = users.get(user.id)
        user_system = first_or_none(users.values(), lambda o: o.is_system)
        if user_system is None: return  # No current system user, so no checks needed
        if user_existing is None:  # new
            if user.is_system:
                msg = "Cannot create system user {user} because system user {user_system} already exists"
                log.warning(msg.format(user=str(user), user_system=str(user_system)))
                raise ValueError(msg.format(user=str(user.username), user_system=str(user_system.username)))
        else:  # existing
            if user.id == user_system.id:
                if not user.is_system:
                    msg = "Cannot make system user {user} a non-system user"
                    log.warning(msg.format(user=str(user)))
                    raise ValueError(msg.format(user=str(user.username)))
            else:
                if user.is_system:
                    msg = "Cannot make user {user} a system user"
                    log.warning(msg.format(user=str(user)))
                    raise ValueError(msg.format(user=str(user.username)))

    @staticmethod
    def save_user_check_duplicate_username(user: User, users: Mapping[UUID, User]):
        user_existing = users.get(user.id)
        user_username = first_or_none(users.values(), lambda o: o.username == user.username)
        if user_username is None: return  # No users with this username
        if user_existing is None:  # new
            msg = "Cannot create user {user} because another user with that same username already exists {user_username}"
            log.warning(msg.format(user=str(user), user_username=str(user_username)))
            raise ValueError(msg.format(user=str(user.username), user_username=str(user_username.username)))
        else:  # existing
            if user.id != user_username.id:
                msg = "Cannot update user {user} because another user with that same username already exists {user_username}"
                log.warning(msg.format(user=str(user), user_username=str(user_username)))
                raise ValueError(msg.format(user=str(user.username), user_username=str(user_username.username)))

    def save_user(self, user: User):
        user.check()
        db = self._db[User]
        users = db.get_all()
        self.save_user_check_system_user(user, users)
        self.save_user_check_duplicate_username(user, users)
        db.save(user)


    @staticmethod
    def delete_user_check_system_user(user: User, users: Mapping[UUID, User]):
        user_system = first_or_none(users.values(), lambda o: o.is_system)
        if user_system is not None:
            if user_system.id == user.id:
                msg = "Cannot delete system user {user}"
                log.warning(msg.format(user=str(user)))
                raise ValueError(msg.format(user=str(user.username)))


    def delete_user(self, user: User):
        user.check()
        db = self._db[User]
        user_existing = db.get_by_id(user.id)
        if self.delete_check_not_exist(user, user_existing): return
        users = db.get_all()
        self.delete_user_check_system_user(user, users)
        db.delete(user)

    def get_users(self) -> Mapping[UUID, User]:
        db = self._db[User]
        return db.get_all()



class DataServiceJob(DataServiceBase, ABC):
    def save_job(self, job: Job):
        job.check()
        db = self._db[Job]
        db.save(job)
        # TODO: validate config references are valid
        # TODO: check and cleanup unused configs

    def delete_job(self, job: Job):
        job.check()
        db = self._db[Job]
        job_existing = db.get_by_id(job.id)
        if self.delete_check_not_exist(job, job_existing): return
        db.delete(job)
        # TODO: check and cleanup unused configs

    def get_jobs(self) -> Mapping[UUID, Job]:
        db = self._db[Job]
        return db.get_all()




class DataService:
    def __init__(
            self,
            db=ModelDatabase(),
            password_encrypt=_default_password_encrypt,
            password_validate=_default_password_validate,
            system_username="admin",
            system_password="admin",
    ):
        self.__db = db
        self._password_encrypt = password_encrypt
        self._password_validate = password_validate
        self._system_username = system_username
        self._system_password = system_password

    @property
    def _db(self):
        return self.__db











