import hashlib
import uuid
from datetime import datetime
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework_simplejwt.tokens import RefreshToken
from core.db import get_collection


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def generate_token(user_id: str, email: str):
    class FakeUser:
        def __init__(self, uid, email):
            self.id = uid
            self.email = email
            self.pk = uid
            self.is_active = True

        @property
        def is_authenticated(self):
            return True

    fake_user = FakeUser(user_id, email)
    refresh = RefreshToken.for_user(fake_user)
    refresh['email'] = email
    return {
        'refresh': str(refresh),
        'access': str(refresh.access_token),
    }


class RegisterView(APIView):
    def post(self, request):
        users = get_collection('users')
        email = request.data.get('email', '').strip().lower()
        password = request.data.get('password', '')
        name = request.data.get('name', '')

        if not email or not password:
            return Response({'error': 'Email and password are required.'}, status=400)

        if users.find_one({'email': email}):
            return Response({'error': 'User already exists.'}, status=409)

        user_id = str(uuid.uuid4())
        users.insert_one({
            'user_id': user_id,
            'email': email,
            'name': name,
            'password_hash': hash_password(password),
            'created_at': datetime.utcnow().isoformat(),
        })

        tokens = generate_token(user_id, email)
        return Response({
            'message': 'Registration successful.',
            'user': {'user_id': user_id, 'email': email, 'name': name},
            **tokens,
        }, status=201)


class LoginView(APIView):
    def post(self, request):
        users = get_collection('users')
        email = request.data.get('email', '').strip().lower()
        password = request.data.get('password', '')

        if not email or not password:
            return Response({'error': 'Email and password are required.'}, status=400)

        user = users.find_one({'email': email})
        if not user or user['password_hash'] != hash_password(password):
            return Response({'error': 'Invalid credentials.'}, status=401)

        tokens = generate_token(user['user_id'], email)
        return Response({
            'message': 'Login successful.',
            'user': {
                'user_id': user['user_id'],
                'email': user['email'],
                'name': user.get('name', ''),
            },
            **tokens,
        })
