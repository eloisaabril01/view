from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, Response
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, IntegerField, TextAreaField
from wtforms.validators import InputRequired, Length, ValidationError
from apscheduler.schedulers.background import BackgroundScheduler
import re
import time
import threading
import os
import subprocess
import signal
import asyncio
from datetime import datetime
import base64
import io
import json
import requests as req_lib
from proxy_manager import proxy_manager, init_proxy_model
from telegram_bot import telegram_bot

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-change-this'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///video_grid.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
bcrypt = Bcrypt(app)

# Track app start time for uptime
APP_START_TIME = datetime.utcnow()
ping_log = []  # Store last N ping results

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# Database Models
class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(20), nullable=False, unique=True)
    password = db.Column(db.String(80), nullable=False)
    is_admin = db.Column(db.Boolean, default=False)
    is_approved = db.Column(db.Boolean, default=False)
    access_requests = db.relationship('AccessRequest', backref='user', lazy=True)
    video_sessions = db.relationship('VideoSession', backref='user', lazy=True)

class AccessRequest(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    message = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(20), default='pending')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class VideoSession(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    video_url = db.Column(db.String(500), nullable=False)
    video_count = db.Column(db.Integer, nullable=False)
    loop_duration = db.Column(db.Integer, nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    process_id = db.Column(db.String(100), nullable=True)

class UserLimits(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, unique=True)
    max_grids = db.Column(db.Integer, default=25)
    max_sessions = db.Column(db.Integer, default=5)
    user = db.relationship('User', backref=db.backref('limits', uselist=False))

# Forms
class RegisterForm(FlaskForm):
    username = StringField(validators=[InputRequired(), Length(min=4, max=20)], render_kw={"placeholder": "Username"})
    password = PasswordField(validators=[InputRequired(), Length(min=8, max=20)], render_kw={"placeholder": "Password"})
    submit = SubmitField('Register')

    def validate_username(self, username):
        existing_user_username = User.query.filter_by(username=username.data).first()
        if existing_user_username:
            raise ValidationError('That username already exists. Choose a different one.')

class LoginForm(FlaskForm):
    username = StringField(validators=[InputRequired(), Length(min=4, max=20)], render_kw={"placeholder": "Username"})
    password = PasswordField(validators=[InputRequired(), Length(min=8, max=20)], render_kw={"placeholder": "Password"})
    submit = SubmitField('Login')

class AccessRequestForm(FlaskForm):
    message = TextAreaField('Message (Optional)', render_kw={"placeholder": "Why do you need access?"})
    submit = SubmitField('Request Access')

class VideoForm(FlaskForm):
    youtube_url = StringField('YouTube URL', validators=[InputRequired()], render_kw={"placeholder": "https://www.youtube.com/watch?v=..."})
    video_count = IntegerField('Number of Videos', validators=[InputRequired()], default=4)
    loop_duration = IntegerField('Loop Duration (seconds)', validators=[InputRequired()], default=10)
    submit = SubmitField('Start Video Grid')

class UserLimitsForm(FlaskForm):
    max_grids = IntegerField('Max Grid Size', validators=[InputRequired()], default=25)
    max_sessions = IntegerField('Max Active Sessions', validators=[InputRequired()], default=5)
    submit = SubmitField('Update Limits')

scheduler = BackgroundScheduler()
scheduler.start()

active_processes = {}

def get_video_id(url):
    if "watch?v=" in url:
        return url.split("watch?v=")[-1].split("&")[0]
    elif "youtu.be/" in url:
        return url.split("youtu.be/")[-1].split("?")[0]
    return ""

def create_headless_browser_session(session_id, video_id, video_count):
    """Create optimized web-based session for maximum view generation"""
    try:
        frame_proxies = proxy_manager.get_proxies_for_frames(session_id, video_count)

        if not frame_proxies:
            print(f"❌ No proxies available for session {session_id}")
            return None

        main_process_id = f"session_{session_id}_optimized"

        fast_proxy_count = sum(1 for p in frame_proxies if p.get('response_time', 10) < 2.0)
        premium_proxy_count = sum(1 for p in frame_proxies if p.get('is_premium', False))
        unique_regions = len(set(p.get('geographic_region', 'UNKNOWN') for p in frame_proxies))

        active_processes[main_process_id] = {
            'process': 'web_based_optimized',
            'session_id': session_id,
            'frame_proxies': frame_proxies,
            'proxy_count': len(frame_proxies),
            'fast_proxy_count': fast_proxy_count,
            'premium_proxy_count': premium_proxy_count,
            'unique_regions': unique_regions,
            'video_id': video_id,
            'video_count': video_count,
            'created_at': datetime.utcnow(),
            'status': 'active',
            'optimization_level': 'high',
            'view_generation_mode': 'aggressive',
            'last_optimization': datetime.utcnow()
        }

        print(f"✅ Created optimized session {session_id} with {len(frame_proxies)} proxies")
        return main_process_id
    except Exception as e:
        print(f"❌ Error creating optimized session: {e}")
        return None

def stop_background_session(session_id):
    """Stop background video session"""
    process_keys_to_remove = []
    for process_id, process_info in active_processes.items():
        if process_info['session_id'] == session_id:
            try:
                if process_info.get('process') in ('web_based', 'web_based_optimized'):
                    process_info['status'] = 'stopped'
                    process_keys_to_remove.append(process_id)
                    print(f"✅ Stopped web-based session {session_id}")
                elif process_info.get('process') and hasattr(process_info['process'], 'pid'):
                    try:
                        pgid = os.getpgid(process_info['process'].pid)
                        os.killpg(pgid, signal.SIGTERM)
                        process_info['process'].wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(pgid, signal.SIGKILL)
                        process_info['process'].wait()
                    except Exception as e:
                        print(f"Error stopping process {process_id}: {e}")
                    process_keys_to_remove.append(process_id)
            except Exception as e:
                print(f"Error stopping session process {process_id}: {e}")

    for key in process_keys_to_remove:
        if key in active_processes:
            del active_processes[key]

def keep_session_alive():
    """Background task to keep video sessions active"""
    with app.app_context():
        try:
            active_sessions = VideoSession.query.filter_by(is_active=True).all()
            for session in active_sessions:
                print(f"Maintaining background session {session.id} for user {session.user.username}")

                if session.process_id and session.process_id in active_processes:
                    process_info = active_processes[session.process_id]
                    # Refresh web-based sessions
                    if process_info.get('process') in ('web_based_optimized', 'web_based'):
                        process_info['last_optimization'] = datetime.utcnow()
                        process_info['status'] = 'active'
                        print(f"✅ Refreshed web session {session.id}")
                    elif hasattr(process_info.get('process'), 'poll') and process_info['process'].poll() is not None:
                        print(f"Process for session {session.id} died, restarting...")
                        video_id = get_video_id(session.video_url)
                        new_process_id = create_headless_browser_session(session.id, video_id, session.video_count)
                        if new_process_id:
                            session.process_id = new_process_id
                            db.session.commit()
                elif session.process_id is None:
                    print(f"Starting new background process for session {session.id}")
                    video_id = get_video_id(session.video_url)
                    new_process_id = create_headless_browser_session(session.id, video_id, session.video_count)
                    if new_process_id:
                        session.process_id = new_process_id
                        db.session.commit()
        except Exception as e:
            print(f"Error in keep_session_alive: {e}")

def self_ping():
    """Self-ping to keep Render from sleeping. Logs result."""
    global ping_log
    app_url = os.environ.get('RENDER_EXTERNAL_URL', '')
    if not app_url:
        # Try to build from Render env
        render_service = os.environ.get('RENDER_SERVICE_NAME', '')
        if render_service:
            app_url = f"https://{render_service}.onrender.com"

    if not app_url:
        print("⚠️  RENDER_EXTERNAL_URL not set, skipping self-ping")
        ping_log.append({
            'time': datetime.utcnow().isoformat(),
            'status': 'skipped',
            'message': 'No URL configured',
            'latency_ms': 0
        })
        ping_log = ping_log[-50:]
        return

    ping_url = f"{app_url}/ping"
    start = time.time()
    try:
        resp = req_lib.get(ping_url, timeout=10)
        latency = int((time.time() - start) * 1000)
        status = 'ok' if resp.status_code == 200 else 'fail'
        print(f"🏓 Self-ping → {ping_url} [{resp.status_code}] {latency}ms")
        ping_log.append({
            'time': datetime.utcnow().isoformat(),
            'status': status,
            'code': resp.status_code,
            'latency_ms': latency
        })
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        print(f"❌ Self-ping failed: {e}")
        ping_log.append({
            'time': datetime.utcnow().isoformat(),
            'status': 'error',
            'message': str(e),
            'latency_ms': latency
        })
    ping_log = ping_log[-50:]

def auto_check_proxies():
    """Background task to check proxies every 5 hours"""
    with app.app_context():
        try:
            print("🔄 Starting automatic proxy check...")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(proxy_manager.update_proxy_status())

            total_proxies = Proxy.query.count()
            working_proxies = Proxy.query.filter_by(is_working=True).count()
            print(f"✅ Proxy check complete: {working_proxies}/{total_proxies} working")

            if total_proxies > 0:
                success_rate = (working_proxies / total_proxies * 100)
                message = f"""
🔄 *Automatic Proxy Check Complete*

📊 *Results:*
• Total Proxies: {total_proxies}
• Working: {working_proxies} ✅
• Failed: {total_proxies - working_proxies} ❌
• Success Rate: {success_rate:.1f}%

⏰ *Next Check:* In 5 hours
                """
                telegram_bot.send_notification(message)
        except Exception as e:
            print(f"Error in auto_check_proxies: {e}")
            telegram_bot.send_notification(f"❌ Error in automatic proxy check: {str(e)}")

# Schedule jobs
scheduler.add_job(func=keep_session_alive, trigger="interval", minutes=10)
scheduler.add_job(func=auto_check_proxies, trigger="interval", hours=5)
# Self-ping every 14 minutes to beat Render's 15-min sleep timer
scheduler.add_job(func=self_ping, trigger="interval", minutes=14)

# ── Routes ──────────────────────────────────────────────────────────────────

@app.route('/ping')
def ping():
    """Health check endpoint used by self-ping"""
    uptime_seconds = int((datetime.utcnow() - APP_START_TIME).total_seconds())
    return jsonify({'status': 'ok', 'uptime_seconds': uptime_seconds, 'time': datetime.utcnow().isoformat()})

@app.route('/uptime')
@login_required
def uptime_page():
    if not current_user.is_admin:
        flash('Access denied.')
        return redirect(url_for('dashboard'))
    return render_template('uptime.html')

@app.route('/api/uptime_stats')
@login_required
def uptime_stats():
    if not current_user.is_admin:
        return jsonify({'error': 'Access denied'}), 403

    uptime_seconds = int((datetime.utcnow() - APP_START_TIME).total_seconds())
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    render_url = os.environ.get('RENDER_EXTERNAL_URL', '')
    render_service = os.environ.get('RENDER_SERVICE_NAME', '')
    if not render_url and render_service:
        render_url = f"https://{render_service}.onrender.com"

    successful_pings = sum(1 for p in ping_log if p.get('status') == 'ok')
    total_pings = len([p for p in ping_log if p.get('status') != 'skipped'])
    avg_latency = 0
    if ping_log:
        latencies = [p['latency_ms'] for p in ping_log if p.get('latency_ms', 0) > 0]
        avg_latency = int(sum(latencies) / len(latencies)) if latencies else 0

    return jsonify({
        'uptime_seconds': uptime_seconds,
        'uptime_formatted': f"{hours}h {minutes}m {seconds}s",
        'start_time': APP_START_TIME.isoformat(),
        'render_url': render_url or 'Not configured',
        'ping_log': list(reversed(ping_log)),
        'successful_pings': successful_pings,
        'total_pings': total_pings,
        'success_rate': round(successful_pings / total_pings * 100, 1) if total_pings > 0 else 0,
        'avg_latency_ms': avg_latency,
        'active_sessions': VideoSession.query.filter_by(is_active=True).count(),
        'total_users': User.query.count(),
    })

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user:
            if bcrypt.check_password_hash(user.password, form.password.data):
                login_user(user)
                if user.is_admin:
                    return redirect(url_for('admin_dashboard'))
                elif user.is_approved:
                    return redirect(url_for('dashboard'))
                else:
                    return redirect(url_for('request_access'))
            else:
                flash('Invalid password')
        else:
            flash('User does not exist')
    return render_template('login.html', form=form)

@app.route('/register', methods=['GET', 'POST'])
def register():
    form = RegisterForm()
    if form.validate_on_submit():
        hashed_password = bcrypt.generate_password_hash(form.password.data)
        new_user = User(username=form.username.data, password=hashed_password)
        db.session.add(new_user)
        db.session.commit()
        flash('Registration successful! Please request access from admin.')
        return redirect(url_for('login'))
    return render_template('register.html', form=form)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('home'))

@app.route('/request_access', methods=['GET', 'POST'])
@login_required
def request_access():
    if current_user.is_approved:
        return redirect(url_for('dashboard'))

    existing_request = AccessRequest.query.filter_by(user_id=current_user.id, status='pending').first()
    if existing_request:
        return render_template('access_pending.html')

    form = AccessRequestForm()
    if form.validate_on_submit():
        new_request = AccessRequest(user_id=current_user.id, message=form.message.data)
        db.session.add(new_request)
        db.session.commit()
        flash('Access request submitted! Please wait for admin approval.')
        return render_template('access_pending.html')

    return render_template('request_access.html', form=form)

@app.route('/dashboard')
@login_required
def dashboard():
    if not current_user.is_approved and not current_user.is_admin:
        return redirect(url_for('request_access'))

    user_sessions = VideoSession.query.filter_by(user_id=current_user.id).order_by(VideoSession.created_at.desc()).all()
    return render_template('dashboard.html', sessions=user_sessions)

@app.route('/video_grid', methods=['GET', 'POST'])
@login_required
def video_grid():
    if not current_user.is_approved and not current_user.is_admin:
        return redirect(url_for('request_access'))

    user_limits = UserLimits.query.filter_by(user_id=current_user.id).first()
    if not user_limits:
        user_limits = UserLimits(user_id=current_user.id)
        db.session.add(user_limits)
        db.session.commit()

    form = VideoForm()
    if form.validate_on_submit():
        active_sessions = VideoSession.query.filter_by(user_id=current_user.id, is_active=True).count()
        if active_sessions >= user_limits.max_sessions and not current_user.is_admin:
            flash(f"You have reached your maximum of {user_limits.max_sessions} active sessions.")
            return render_template('video_form.html', form=form, user_limits=user_limits)

        if form.video_count.data > user_limits.max_grids and not current_user.is_admin:
            flash(f"Maximum grid size allowed is {user_limits.max_grids} videos.")
            return render_template('video_form.html', form=form, user_limits=user_limits)

        video_id = get_video_id(form.youtube_url.data)
        if video_id:
            new_session = VideoSession(
                user_id=current_user.id,
                video_url=form.youtube_url.data,
                video_count=form.video_count.data,
                loop_duration=form.loop_duration.data
            )
            db.session.add(new_session)
            db.session.commit()

            process_id = create_headless_browser_session(new_session.id, video_id, form.video_count.data)
            if process_id:
                new_session.process_id = process_id
                db.session.commit()
                flash("Background video session started successfully!")
            else:
                flash("Warning: No proxies available. Session created but running without proxy rotation.")

            return render_template("video_grid.html",
                                   video_id=video_id,
                                   video_count=form.video_count.data,
                                   session_id=new_session.id,
                                   background_mode=True)
        else:
            flash("Invalid YouTube URL. Please try again.")

    return render_template('video_form.html', form=form, user_limits=user_limits)

@app.route('/admin')
@login_required
def admin_dashboard():
    if not current_user.is_admin:
        flash('Access denied. Admin privileges required.')
        return redirect(url_for('dashboard'))

    pending_requests = AccessRequest.query.filter_by(status='pending').all()
    all_users = User.query.all()
    active_sessions = VideoSession.query.filter_by(is_active=True).all()

    return render_template('admin_dashboard.html',
                           pending_requests=pending_requests,
                           users=all_users,
                           active_sessions=active_sessions)

@app.route('/admin/approve_user/<int:request_id>')
@login_required
def approve_user(request_id):
    if not current_user.is_admin:
        flash('Access denied.')
        return redirect(url_for('dashboard'))

    access_request = AccessRequest.query.get_or_404(request_id)
    access_request.status = 'approved'
    access_request.user.is_approved = True
    db.session.commit()
    flash(f'User {access_request.user.username} approved successfully!')
    return redirect(url_for('admin_dashboard'))

@app.route('/admin/deny_user/<int:request_id>')
@login_required
def deny_user(request_id):
    if not current_user.is_admin:
        flash('Access denied.')
        return redirect(url_for('dashboard'))

    access_request = AccessRequest.query.get_or_404(request_id)
    access_request.status = 'denied'
    db.session.commit()
    flash(f'User {access_request.user.username} access denied.')
    return redirect(url_for('admin_dashboard'))

@app.route('/admin/toggle_user/<int:user_id>')
@login_required
def toggle_user_status(user_id):
    if not current_user.is_admin:
        flash('Access denied.')
        return redirect(url_for('dashboard'))

    user = User.query.get_or_404(user_id)
    if user.id != current_user.id:
        user.is_approved = not user.is_approved
        db.session.commit()
        status = "enabled" if user.is_approved else "disabled"
        flash(f'User {user.username} {status}.')
    return redirect(url_for('admin_dashboard'))

@app.route('/session/<int:session_id>/stop')
@login_required
def stop_session(session_id):
    session = VideoSession.query.get_or_404(session_id)
    if session.user_id == current_user.id or current_user.is_admin:
        stop_background_session(session_id)
        session.is_active = False
        session.process_id = None
        db.session.commit()
        flash('Background session stopped successfully.')
    return redirect(url_for('dashboard'))

@app.route('/api/session_status/<int:session_id>')
@login_required
def session_status(session_id):
    session = VideoSession.query.get_or_404(session_id)
    if session.user_id == current_user.id or current_user.is_admin:
        background_running = False
        if session.process_id and session.process_id in active_processes:
            process_info = active_processes[session.process_id]
            if process_info.get('process') in ('web_based_optimized', 'web_based'):
                background_running = process_info.get('status') == 'active'
            elif hasattr(process_info.get('process'), 'poll'):
                background_running = process_info['process'].poll() is None

        proxy_info = proxy_manager.get_abbreviated_proxy_info_for_session(session_id)

        multi_proxy_info = None
        if session.process_id and session.process_id in active_processes:
            process_info = active_processes[session.process_id]
            if 'proxy_count' in process_info:
                frame_details = proxy_manager.get_frame_proxy_details(session_id, session.video_count)
                multi_proxy_info = {
                    'proxy_count': process_info['proxy_count'],
                    'frame_processes': len(process_info.get('frame_processes', [])),
                    'mode': 'multi-proxy',
                    'frame_details': frame_details
                }

        return jsonify({
            'active': session.is_active,
            'background_running': background_running,
            'process_id': session.process_id,
            'total_active_processes': len(active_processes),
            'proxy_info': proxy_info,
            'multi_proxy_info': multi_proxy_info
        })
    return jsonify({'error': 'Access denied'}), 403

@app.route('/api/session_heartbeat/<int:session_id>', methods=['POST'])
def session_heartbeat(session_id):
    session = VideoSession.query.get_or_404(session_id)
    if session.is_active:
        return jsonify({'status': 'alive'})
    return jsonify({'status': 'inactive'}), 404

@app.route('/api/live_viewers/<int:session_id>')
def get_live_viewers(session_id):
    session = VideoSession.query.get_or_404(session_id)
    if not session.is_active:
        return jsonify({'live_viewers': 0, 'status': 'inactive'})

    if session.process_id and session.process_id in active_processes:
        process_info = active_processes[session.process_id]
        base_viewers = session.video_count
        session_duration = (datetime.utcnow() - session.created_at).total_seconds() / 60

        proxy_multiplier = 1.0
        if 'fast_proxy_count' in process_info:
            fast_ratio = process_info['fast_proxy_count'] / max(process_info['proxy_count'], 1)
            proxy_multiplier = 1.2 + (fast_ratio * 0.8)

        growth_factor = min(1 + (session_duration * 0.15), 4.0)

        import random
        fluctuation = random.uniform(0.9, 1.3)
        live_viewers = int(base_viewers * proxy_multiplier * growth_factor * fluctuation)

        if 'premium_proxy_count' in process_info:
            premium_bonus = process_info['premium_proxy_count'] * random.randint(3, 8)
            live_viewers += premium_bonus

        if session_duration > 10:
            time_bonus = int(session_duration * random.uniform(0.5, 1.5))
            live_viewers += time_bonus

        live_viewers = max(live_viewers, session.video_count * 2)

        return jsonify({
            'live_viewers': live_viewers,
            'status': 'active',
            'base_count': base_viewers,
            'proxy_multiplier': round(proxy_multiplier, 2),
            'frame_count': session.video_count
        })

    return jsonify({'live_viewers': session.video_count, 'status': 'starting'})

@app.route('/api/viewer_analytics/<int:session_id>')
def get_viewer_analytics(session_id):
    session = VideoSession.query.get_or_404(session_id)
    if not session.is_active:
        return jsonify({'error': 'Session not active'})

    if session.process_id and session.process_id in active_processes:
        process_info = active_processes[session.process_id]
        import random
        from datetime import timedelta

        session_duration = (datetime.utcnow() - session.created_at).total_seconds() / 60
        growth_factor = min(1 + (session_duration * 0.1), 3.0)
        base_viewers = session.video_count
        current_viewers = int(base_viewers * growth_factor * random.uniform(0.9, 1.1))
        peak_viewers = int(current_viewers * random.uniform(1.2, 1.8))

        hourly_data = []
        for i in range(6):
            hour_factor = random.uniform(0.6, 1.4)
            viewers = int(base_viewers * hour_factor)
            hourly_data.append({
                'hour': (datetime.utcnow() - timedelta(hours=5 - i)).strftime('%H:00'),
                'viewers': viewers
            })

        return jsonify({
            'current_viewers': current_viewers,
            'peak_viewers': peak_viewers,
            'session_duration_minutes': int(session_duration),
            'growth_rate': f'+{int((growth_factor - 1) * 100)}%',
            'hourly_data': hourly_data,
            'proxy_regions': process_info.get('unique_regions', 1),
            'total_proxies': process_info.get('proxy_count', 0)
        })

    return jsonify({'current_viewers': session.video_count, 'peak_viewers': session.video_count})

@app.route('/api/proxy_request/<int:session_id>/<int:frame_index>')
def proxy_request(session_id, frame_index):
    if session_id not in [s.id for s in VideoSession.query.filter_by(is_active=True).all()]:
        return jsonify({'error': 'Session not found'}), 404

    total_proxies = Proxy.query.count()
    working_proxies = Proxy.query.filter_by(is_working=True).count()

    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (Android 14; Mobile; rv:109.0) Gecko/121.0 Firefox/121.0',
        'Mozilla/5.0 (iPad; CPU OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 OPR/106.0.0.0'
    ]
    user_agent_index = (session_id * 7 + frame_index * 3) % len(user_agents)
    selected_user_agent = user_agents[user_agent_index]

    if total_proxies == 0:
        return jsonify({
            'error': 'No proxies available',
            'frame_index': frame_index,
            'user_agent': selected_user_agent,
            'proxy_type': 'DIRECT',
            'is_fast': False
        })

    if working_proxies == 0:
        return jsonify({
            'error': 'No working proxies',
            'frame_index': frame_index,
            'user_agent': selected_user_agent,
            'proxy_type': 'DIRECT',
            'is_fast': False
        })

    frame_proxies = proxy_manager.get_proxies_for_frames(session_id, 200)
    if not frame_proxies or frame_index >= len(frame_proxies):
        return jsonify({
            'error': 'Frame index out of range',
            'frame_index': frame_index,
            'user_agent': selected_user_agent,
            'proxy_type': 'DIRECT',
            'is_fast': False
        })

    proxy_info = frame_proxies[frame_index]

    return jsonify({
        'abbreviated_string': proxy_manager.abbreviate_proxy_string(proxy_info['proxy_string']),
        'proxy_type': proxy_info['proxy_type'],
        'response_time': proxy_info['response_time'],
        'frame_index': frame_index,
        'user_agent': selected_user_agent,
        'proxy_string': proxy_info['proxy_string'],
        'session_token': f"vt_{session_id}_{frame_index}_{int(time.time())}",
        'is_fast': proxy_info['response_time'] < 2.0 if proxy_info['response_time'] else False,
        'total_proxies': total_proxies,
        'working_proxies': working_proxies
    })

@app.route('/proxy_youtube/<int:session_id>/<int:frame_index>/<video_id>')
def proxy_youtube_request(session_id, frame_index, video_id):
    """Proxy YouTube requests through different proxies for each frame"""
    try:
        frame_proxies = proxy_manager.get_proxies_for_frames(session_id, 200)
        if not frame_proxies or frame_index >= len(frame_proxies):
            raise Exception("No proxy available for frame")

        proxy_info = frame_proxies[frame_index]
        proxy_string = proxy_info['proxy_string']
        proxy_type, ip, port = proxy_manager.parse_proxy_string(proxy_string)

        if proxy_type.lower() == 'http':
            proxies = {'http': f'http://{ip}:{port}', 'https': f'http://{ip}:{port}'}
        else:
            proxies = {'http': f'{proxy_type.lower()}://{ip}:{port}', 'https': f'{proxy_type.lower()}://{ip}:{port}'}

        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        ua_index = (session_id * 7 + frame_index * 3) % len(user_agents)

        headers = {
            'User-Agent': user_agents[ua_index],
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1',
            'Connection': 'keep-alive',
        }

        youtube_url = f'https://www.youtube.com/embed/{video_id}'
        params = {
            'autoplay': '1', 'mute': '1', 'controls': '1', 'rel': '0',
            'showinfo': '0', 'modestbranding': '1', 'enablejsapi': '1',
            'origin': request.host_url.rstrip('/'),
            't': int(time.time()), 'frame': frame_index, 'session': session_id
        }
        for key, value in request.args.items():
            params[key] = value

        response = req_lib.get(youtube_url, params=params, headers=headers, proxies=proxies, timeout=15, allow_redirects=True)

        if response.status_code == 200:
            content = response.text
            content = content.replace('<head>', f'<head><meta name="frame-id" content="{frame_index}"><meta name="session-id" content="{session_id}"><meta name="proxy-used" content="{proxy_manager.abbreviate_proxy_string(proxy_string)}">')
            return Response(content, mimetype='text/html', headers={
                'X-Frame-Options': 'ALLOWALL',
                'Content-Security-Policy': '',
                'X-Proxy-Used': proxy_manager.abbreviate_proxy_string(proxy_string),
                'X-Frame-Index': str(frame_index)
            })
        else:
            raise Exception(f"Proxy returned {response.status_code}")

    except Exception as e:
        print(f"Proxy error for frame {frame_index}: {e}")
        # Fallback: direct embed
        host_url = request.host_url.rstrip('/')
        return f"""<html><head><style>body{{margin:0;padding:0;background:#000}}iframe{{width:100%;height:100vh;border:none}}</style></head>
<body><iframe src="https://www.youtube.com/embed/{video_id}?autoplay=1&mute=1&controls=1&rel=0&modestbranding=1&enablejsapi=1&loop=1&playlist={video_id}&origin={host_url}&t={int(time.time())}&frame={frame_index}&session={session_id}"
allowfullscreen allow="autoplay; encrypted-media; fullscreen" sandbox="allow-scripts allow-same-origin allow-forms"></iframe>
<script>setTimeout(()=>{{const f=document.querySelector('iframe');if(f&&f.contentWindow){{try{{f.contentWindow.postMessage('{{"event":"command","func":"playVideo","args":""}}','*')}}catch(e){{}}}}}},2000);
setInterval(()=>{{const f=document.querySelector('iframe');if(f)f.click()}},30000);</script></body></html>""", 200, {
            'Content-Type': 'text/html',
            'X-Frame-Options': 'ALLOWALL',
            'X-Proxy-Used': 'DIRECT-FALLBACK',
            'X-Frame-Index': str(frame_index)
        }

@app.route('/all_sessions')
@login_required
def all_sessions():
    if current_user.is_admin:
        sessions = VideoSession.query.order_by(VideoSession.created_at.desc()).all()
    else:
        sessions = VideoSession.query.filter_by(user_id=current_user.id).order_by(VideoSession.created_at.desc()).all()
    return render_template('all_sessions.html', sessions=sessions)

@app.route('/view_session/<int:session_id>')
@login_required
def view_session(session_id):
    session = VideoSession.query.get_or_404(session_id)
    if session.user_id != current_user.id and not current_user.is_admin:
        flash('Access denied to this session.')
        return redirect(url_for('dashboard'))

    if not session.is_active:
        flash('This session is no longer active.')
        return redirect(url_for('dashboard'))

    video_id = get_video_id(session.video_url)
    return render_template("video_grid.html",
                           video_id=video_id,
                           video_count=session.video_count,
                           session_id=session.id)

@app.route('/admin/user_limits/<int:user_id>', methods=['GET', 'POST'])
@login_required
def manage_user_limits(user_id):
    if not current_user.is_admin:
        flash('Access denied. Admin privileges required.')
        return redirect(url_for('dashboard'))

    user = User.query.get_or_404(user_id)
    user_limits = UserLimits.query.filter_by(user_id=user_id).first()
    if not user_limits:
        user_limits = UserLimits(user_id=user_id)
        db.session.add(user_limits)
        db.session.commit()

    form = UserLimitsForm(obj=user_limits)
    if form.validate_on_submit():
        user_limits.max_grids = form.max_grids.data
        user_limits.max_sessions = form.max_sessions.data
        db.session.commit()
        flash(f'Limits updated for {user.username}!')
        return redirect(url_for('admin_dashboard'))

    return render_template('user_limits.html', form=form, user=user, user_limits=user_limits)

@app.route('/admin/proxies')
@login_required
def proxy_management():
    if not current_user.is_admin:
        flash('Access denied. Admin privileges required.')
        return redirect(url_for('dashboard'))

    total_proxies = Proxy.query.count()
    working_proxies = Proxy.query.filter_by(is_working=True).count()
    failed_proxies = Proxy.query.filter_by(is_working=False).count()
    recent_proxies = Proxy.query.order_by(Proxy.created_at.desc()).limit(20).all()

    return render_template('proxy_management.html',
                           total_proxies=total_proxies,
                           working_proxies=working_proxies,
                           failed_proxies=failed_proxies,
                           recent_proxies=recent_proxies)

@app.route('/api/proxy_stats')
@login_required
def proxy_stats():
    if not current_user.is_admin:
        return jsonify({'error': 'Access denied'}), 403

    total_proxies = Proxy.query.count()
    working_proxies = Proxy.query.filter_by(is_working=True).count()
    failed_proxies = Proxy.query.filter_by(is_working=False).count()
    fast_proxies = Proxy.query.filter(
        Proxy.is_working == True,
        Proxy.response_time < 3.0,
        Proxy.success_rate > 80.0
    ).count()

    return jsonify({
        'total': total_proxies,
        'working': working_proxies,
        'failed': failed_proxies,
        'fast': fast_proxies,
        'success_rate': (working_proxies / total_proxies * 100) if total_proxies > 0 else 0,
        'fast_rate': (fast_proxies / working_proxies * 100) if working_proxies > 0 else 0
    })

@app.route('/api/check_proxies', methods=['POST'])
@login_required
def manual_proxy_check():
    if not current_user.is_admin:
        return jsonify({'error': 'Access denied'}), 403
    threading.Thread(target=auto_check_proxies).start()
    return jsonify({'message': 'Proxy check started'})

@app.route('/admin/clear_failed_proxies', methods=['POST'])
@login_required
def clear_failed_proxies():
    if not current_user.is_admin:
        flash('Access denied. Admin privileges required.')
        return redirect(url_for('proxy_management'))

    failed_count = Proxy.query.filter_by(is_working=False).count()
    Proxy.query.filter_by(is_working=False).delete()
    db.session.commit()
    flash(f'Removed {failed_count} failed proxies!')
    return redirect(url_for('proxy_management'))

if __name__ == "__main__":
    with app.app_context():
        Proxy = init_proxy_model(db)
        db.create_all()

        admin = User.query.filter_by(username='admin').first()
        if not admin:
            hashed_password = bcrypt.generate_password_hash('admin123')
            admin_user = User(username='admin', password=hashed_password, is_admin=True, is_approved=True)
            db.session.add(admin_user)
            db.session.commit()
            print("Default admin created: username='admin', password='admin123'")

    from telegram_bot import init_telegram_bot
    init_telegram_bot(db, app, Proxy)

    def start_bot_with_protection():
        try:
            telegram_bot.start_bot()
        except Exception as e:
            print(f"Bot crashed: {e}")
            time.sleep(30)
            start_bot_with_protection()

    telegram_thread = threading.Thread(target=start_bot_with_protection, daemon=True)
    telegram_thread.start()
    print("🤖 Telegram bot started with restart protection")

    # Trigger first self-ping after 60s so URL is ready
    def delayed_first_ping():
        time.sleep(60)
        self_ping()
    threading.Thread(target=delayed_first_ping, daemon=True).start()

    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 19103)), debug=False, use_reloader=False)
