#!/usr/bin/env bash
# Arrêter le script en cas d'erreur
set -o errexit

echo "--- Début du Build RiskAgri ---"

# 1. Installer les dépendances Python (Flask, Gunicorn, etc.)
pip install -r requirements.txt

# 2. Créer les dossiers nécessaires
mkdir -p piper
mkdir -p static/audio

# 3. Télécharger et installer Piper pour Linux (indispensable pour Render)
if [ ! -f "./piper/piper" ]; then
    echo "Téléchargement du moteur Piper Linux..."
    curl -L https://github.com/rhasspy/piper/releases/download/2023.11.14-2/piper_linux_x86_64.tar.gz | tar -C piper -xz --strip-components 1
    chmod +x piper/piper
    echo "Piper Linux installé avec succès."
else
    echo "Piper est déjà présent."
fi

echo "--- Build Terminé ---"