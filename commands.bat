@echo off
echo ========================================
echo Deploy Kubernetes
echo ========================================
echo.

REM 1. Elimina cluster esistente
echo [1/9] Eliminazione cluster esistente...
kind delete cluster --name bigdata-project
if errorlevel 1 (
    echo ATTENZIONE: Errore durante l'eliminazione del cluster
)
echo.

REM 2. Crea cluster
echo [2/9] Creazione nuovo cluster...
kind create cluster --config kind/kind_config.yaml
if errorlevel 1 (
    echo ERRORE: Creazione cluster fallita
    exit /b 1
)
echo.

REM 3. Installa NGINX Ingress Controller
echo [3/9] Installazione NGINX Ingress Controller...
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
if errorlevel 1 (
    echo ERRORE: Installazione NGINX fallita
    exit /b 1
)
echo.

REM 4. Build e caricamento User Manager
echo [4/9] Build User Manager...
docker build -t user-manager:latest ./user_manager
if errorlevel 1 (
    echo ERRORE: Build User Manager fallita
    exit /b 1
)
kind load docker-image user-manager:latest --name bigdata-project
echo.

REM 5. Build e caricamento Data Collector
echo [5/9] Build Data Collector...
docker build -t data-collector:latest ./data_collector
if errorlevel 1 (
    echo ERRORE: Build Data Collector fallita
    exit /b 1
)
kind load docker-image data-collector:latest --name bigdata-project
echo.

REM 6. Build e caricamento Alert System
echo [6/9] Build Alert System...
docker build -t alert-system:latest ./alert_system
if errorlevel 1 (
    echo ERRORE: Build Alert System fallita
    exit /b 1
)
kind load docker-image alert-system:latest --name bigdata-project
echo.

REM 7. Build e caricamento Notifier System
echo [7/9] Build Notifier System...
docker build -t notifier-system:latest ./notifier_system
if errorlevel 1 (
    echo ERRORE: Build Notifier System fallita
    exit /b 1
)
kind load docker-image notifier-system:latest --name bigdata-project
echo.

REM 8. Applicazione namespace
echo [8/9] Creazione namespace...
kubectl apply -f kubernetes/namespace.yaml
if errorlevel 1 (
    echo ERRORE: Creazione namespace fallita
    exit /b 1
)
echo.

REM 9. Applicazione configurazioni Kubernetes
echo [9/9] Applicazione configurazioni Kubernetes...
kubectl apply -f kubernetes/
if errorlevel 1 (
    echo ERRORE: Applicazione configurazioni fallita
    exit /b 1
)
echo.

echo ========================================
echo Deploy completato con successo!
echo ========================================
pause