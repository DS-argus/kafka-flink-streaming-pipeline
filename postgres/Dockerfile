FROM  postgres:15

# ① 스크립트를 postgres:postgres 소유로 복사 → COPY --chown 은 Docker 17.09+ 지원
COPY --chown=postgres:postgres postgres/init-scripts/ /docker-entrypoint-initdb.d/

# ② 전체 디렉터리 + 파일에 실행·읽기 권한 부여
#    -R 옵션으로 한 번에 처리
RUN chmod -R 755 /docker-entrypoint-initdb.d