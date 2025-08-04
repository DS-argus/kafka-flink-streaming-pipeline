#!/bin/bash

# 플랫폼별 스트리밍 시작 스크립트

echo "🎯 Kafka 플랫폼별 스트리밍 시작"
echo "=================================="

echo ""
echo "📱 스트리밍할 플랫폼 선택:"
echo "1. Desktop만"
echo "2. Mobile만"
echo "3. Tablet만"
echo "4. Desktop + Mobile"
echo "5. Desktop + Tablet"
echo "6. Mobile + Tablet"
echo "7. 모든 플랫폼 (Desktop + Mobile + Tablet)"

read -p "선택하세요 (1-7): " PLATFORM_CHOICE

# 플랫폼 선택에 따른 서비스 설정
case $PLATFORM_CHOICE in
    1)
        SERVICES="desktop-streamer"
        echo "✅ Desktop 스트리밍 시작"
        ;;
    2)
        SERVICES="mobile-streamer"
        echo "✅ Mobile 스트리밍 시작"
        ;;
    3)
        SERVICES="tablet-streamer"
        echo "✅ Tablet 스트리밍 시작"
        ;;
    4)
        SERVICES="desktop-streamer mobile-streamer"
        echo "✅ Desktop + Mobile 스트리밍 시작"
        ;;
    5)
        SERVICES="desktop-streamer tablet-streamer"
        echo "✅ Desktop + Tablet 스트리밍 시작"
        ;;
    6)
        SERVICES="mobile-streamer tablet-streamer"
        echo "✅ Mobile + Tablet 스트리밍 시작"
        ;;
    7)
        SERVICES="desktop-streamer mobile-streamer tablet-streamer"
        echo "✅ 모든 플랫폼 스트리밍 시작"
        ;;
    *)
        echo "❌ 잘못된 선택입니다. 1-7 중에서 선택하세요."
        exit 1
        ;;
esac

echo ""
echo "📋 선택된 플랫폼: $SERVICES"
echo "📝 설정은 streaming/kafka_config.yml 파일에서 관리됩니다."
echo ""

# Docker Compose로 스트리밍 시작
echo "🚀 Docker Compose로 스트리밍 시작..."
docker compose up -d $SERVICES

echo ""
echo "✅ 스트리밍이 시작되었습니다!"
echo "📊 모니터링:"
echo "   - Kafka UI: http://localhost:8080"
echo "   - Flink UI: http://localhost:8081"
echo "   - Prometheus: http://localhost:9090"
echo ""
echo "📝 로그 확인:"
echo "   docker compose logs -f desktop-streamer"
echo "   docker compose logs -f mobile-streamer"
echo "   docker compose logs -f tablet-streamer"
echo ""
echo "⚙️  설정 변경: streaming/kafka_config.yml 파일을 수정하세요." 