#!/bin/bash

# 플랫폼별 스트리밍 중지 스크립트

echo "🛑 Kafka 플랫폼별 스트리밍 중지"
echo "================================"

echo ""
echo "📱 중지할 플랫폼 선택:"
echo "1. Desktop만"
echo "2. Mobile만"
echo "3. Tablet만"
echo "4. Desktop + Mobile"
echo "5. Desktop + Tablet"
echo "6. Mobile + Tablet"
echo "7. 모든 플랫폼 (Desktop + Mobile + Tablet)"

read -p "선택하세요 (1-7): " STOP_CHOICE

# 중지할 서비스 설정
case $STOP_CHOICE in
    1)
        SERVICES="desktop-streamer"
        PROFILES="desktop"
        echo "✅ Desktop 스트리밍 중지"
        ;;
    2)
        SERVICES="mobile-streamer"
        PROFILES="mobile"
        echo "✅ Mobile 스트리밍 중지"
        ;;
    3)
        SERVICES="tablet-streamer"
        PROFILES="tablet"
        echo "✅ Tablet 스트리밍 중지"
        ;;
    4)
        SERVICES="desktop-streamer mobile-streamer"
        PROFILES="desktop mobile"
        echo "✅ Desktop + Mobile 스트리밍 중지"
        ;;
    5)
        SERVICES="desktop-streamer tablet-streamer"
        PROFILES="desktop tablet"
        echo "✅ Desktop + Tablet 스트리밍 중지"
        ;;
    6)
        SERVICES="mobile-streamer tablet-streamer"
        PROFILES="mobile tablet"
        echo "✅ Mobile + Tablet 스트리밍 중지"
        ;;
    7)
        SERVICES="desktop-streamer mobile-streamer tablet-streamer"
        PROFILES="desktop mobile tablet"
        echo "✅ 모든 플랫폼 스트리밍 중지"
        ;;
    *)
        echo "❌ 잘못된 선택입니다. 1-7 중에서 선택하세요."
        exit 1
        ;;
esac

echo ""
echo "🛑 선택된 서비스 중지 중..."
docker compose stop $SERVICES

echo ""
echo "🗑️  선택된 서비스 컨테이너 제거 중..."
docker compose rm -f $SERVICES

echo ""
echo "✅ 스트리밍이 중지되었습니다!"
echo ""
echo "📝 현재 실행 중인 서비스 확인:"
echo "   docker compose ps"
echo ""
echo "📝 모든 서비스 중지 (Kafka, Flink 등 포함):"
echo "   docker compose down" 