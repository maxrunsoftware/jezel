#  Copyright (C) 2022 Max Run Software (dev@maxrunsoftware.com)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import random

from flask import Flask, Blueprint, redirect, render_template, request, url_for
from flask_bootstrap import Bootstrap5
import flask_login
from flask_wtf import CSRFProtect, FlaskForm
from pprint import pprint

from wtforms import BooleanField, PasswordField, StringField, SubmitField
from wtforms.validators import DataRequired, Length

from config import Config
from models import get_database, get_model_database, Task
from utils import *

from utils_app import BootstrapColor, convert_multidic, flash, render

log = logging.getLogger(__name__)

login_manager = flask_login.LoginManager()
login_manager.login_view = ".login"
login_manager.login_message = None

csrf = CSRFProtect()
bootstrap = Bootstrap5()

bp = Blueprint(
    Config.PROJECT_NAME.casefold() + '_bp',
    __name__,
    template_folder='templates',
    static_folder='static',
)

bp.login_manager = login_manager


class User(flask_login.UserMixin):
    pass


@bp.context_processor
def context_processor():
    def format_price(amount, currency="€"):
        return f"{amount:.2f}{currency}"

    def tags_display(tags):
        return ", ".join([(o.name + "=" + xstr(o.value)) for o in sorted(tags, key=lambda o: o.name.casefold())])


    d = dict(
        # system functions
        enumerate=enumerate,
        next=next,
        len=len,
        pprint=pprint,
        vars=vars,
        dir=dir,
        type=type,

        # config values
        DEBUG=Config.DEBUG,

        # session vars
        models=get_model_database(),

        # Model Types
        Task=Task,

        # Model Func Helpers
        tags_display=tags_display,

        # custom functions
        format_price=format_price,
        color_groups=BootstrapColor.colors_str(),
        xstr=xstr,
        trim=trim,
        str2base64=str2base64,
        json2base64=json2base64,
        json2str=json2str,
    )

    # for k, v in sorted(d.items()): log.debug(f"Registering context_processor '{k}' -> {v}")
    return d


def init_app_web():
    """
    Initialize the core application.
    """

    # init app
    app = Flask(__name__, instance_relative_config=False)
    c = {
        "ENV": "development" if Config.DEBUG else "production",
        "SECRET_KEY": Config.WEB_SECRET_KEY,
        "DEBUG": Config.DEBUG,
        "DATABASE_URI": Config.DATABASE_URI,
        "SESSION_COOKIE_NAME": Config.WEB_SESSION_COOKIE_NAME,
        "SESSION_COOKIE_DOMAIN": Config.WEB_SESSION_COOKIE_DOMAIN,
        "SERVER_NAME": Config.WEB_SERVER_NAME,
        "TESTING": Config.DEBUG,

    }
    for k, v in c.copy().items():
        if not k.casefold().startswith("FLASK_".casefold()):
            c["FLASK_" + k] = v  # TODO: Not sure if FLASK_ prefix is needed
    for k, v in c.items():
        app.config[k] = v

    # set default button sytle and size, will be overwritten by macro parameters
    app.config['BOOTSTRAP_BTN_STYLE'] = 'primary'
    app.config['BOOTSTRAP_BTN_SIZE'] = 'sm'
    # app.config['BOOTSTRAP_BOOTSWATCH_THEME'] = 'lumen'  # uncomment this line to test bootswatch theme

    # set default icon title of table actions
    app.config['BOOTSTRAP_TABLE_VIEW_TITLE'] = 'Read'
    app.config['BOOTSTRAP_TABLE_EDIT_TITLE'] = 'Update'
    app.config['BOOTSTRAP_TABLE_DELETE_TITLE'] = 'Remove'
    app.config['BOOTSTRAP_TABLE_NEW_TITLE'] = 'Create'

    login_manager.init_app(app)
    csrf.init_app(app)
    bootstrap.init_app(app)

    app.register_blueprint(bp)

    with app.app_context():
        db = get_model_database()
        if Config.DEBUG and db.db.is_memory_database:
            db.db.recreate_tables()
            db.create_random_items()
        else:
            db.db.create_tables()

        return app


@bp.route('/')
@flask_login.login_required
def index():
    return render(render_template('index.html'))



class LoginForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired(), Length(1, 100)])
    password = PasswordField('Password', validators=[Length(0, 100)])
    remember = BooleanField('Remember me')
    submit = SubmitField()


users = {'admin': None}


@bp.login_manager.user_loader
def user_loader(username):
    if username not in users: return None
    user = User()
    user.id = username
    return user


def user_auth(form: LoginForm) -> User | None:
    u = trim(str(form.username.data))
    p = trim(str(form.password.data))
    log.debug(f"Username: {u}   Password: {p}")
    u_valid = p_valid = False

    if u in users:
        u_valid = True
        if p == users[u]:
            p_valid = True

    log.debug(f"UsernameValid:{u_valid}  PasswordValid:{p_valid}")
    user = None
    if Config.DEBUG:
        if u_valid:
            p_valid = True

    if u_valid and p_valid: user = user_loader(u)
    return user


@bp.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if not form.validate_on_submit(): return render_template('login.html', form=form)

    log.debug("Calling: login()")

    user = user_auth(form)
    if user is None:
        flash('Invalid Username or Password', BootstrapColor.RED)
        return render_template('login.html', form=form)

    flask_login.login_user(user, remember=form.remember.data)
    flash('Login Successful', BootstrapColor.GREEN)
    return redirect(url_for('.index'))


@bp.route('/logout')
def logout():
    flask_login.logout_user()
    return render_template('logout.html')


@bp.route('/info')
@flask_login.login_required
def info():
    return render(render_template(
        'info.html',
        config_dict=Config.attributes(),
    ))


@bp.route('/database')
@flask_login.login_required
def database():
    return render(render_template(
        'database.html',
    ))

@bp.route('/tasks')
@flask_login.login_required
def tasks():
    db = get_model_database()
    lis = []
    task_list = db.get_tasks()
    task_execution_list = db.get_task_executions()

    def get_last_execution(task_id):
        last_execution = None
        for task_execution in task_execution_list:
            if task_execution.task is None: continue
            if task_execution.task.id != task_id: continue

            if last_execution is None:
                last_execution = task_execution
            else:
                if last_execution.created_on is None:
                    last_execution = task_execution
                elif task_execution.created_on is None:
                    continue
                elif task_execution.created_on > last_execution.created_on:
                    last_execution = task_execution
        return last_execution

    for t in task_list:
        lis.append(t)
        t.last_execution = get_last_execution(t.id)

    return render(render_template(
        'tasks.html',
        tasks=lis,
    ))
