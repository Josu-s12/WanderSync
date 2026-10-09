FROM nginxinc/nginx-unprivileged:1.29-alpine

# Seguridad: imagen de nginx que corre SIN root (puerto 8080)
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html styles.css app.js /usr/share/nginx/html/

EXPOSE 8080
