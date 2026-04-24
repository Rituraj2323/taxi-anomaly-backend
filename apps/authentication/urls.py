from django.urls import path
from .views import RegisterView, LoginView

urlpatterns = [
    path('register/', RegisterView.as_view(), name='auth-register'),
    path('register', RegisterView.as_view(), name='auth-register-noslash'),
    path('login/', LoginView.as_view(), name='auth-login'),
    path('login', LoginView.as_view(), name='auth-login-noslash'),
]
