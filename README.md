<div align="center">
  <h1>🤖 AI Intelligence CRM — Batch Server</h1>
  <p><strong>AI 기반 상담 기록 관리 시스템의 대용량 데이터 처리 엔진</strong></p>
  <p>
    <img src="https://img.shields.io/badge/Java%2017-007396?style=flat-square&logo=openjdk&logoColor=white"/>
    <img src="https://img.shields.io/badge/Spring_Boot%203.5-6DB33F?style=flat-square&logo=spring-boot&logoColor=white"/>
    <img src="https://img.shields.io/badge/Spring_Batch-6DB33F?style=flat-square&logo=spring&logoColor=white"/>
    <img src="https://img.shields.io/badge/Gemini_AI-4285F4?style=flat-square&logo=google&logoColor=white"/>
    <img src="https://img.shields.io/badge/Elasticsearch-005571?style=flat-square&logo=elasticsearch&logoColor=white"/>
    <img src="https://img.shields.io/badge/MongoDB-47A248?style=flat-square&logo=mongodb&logoColor=white"/>
    <img src="https://img.shields.io/badge/Redis-DC382D?style=flat-square&logo=redis&logoColor=white"/>
    <img src="https://img.shields.io/badge/MySQL-4479A1?style=flat-square&logo=mysql&logoColor=white"/>
    <img src="https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white"/>
  </p>
  <p>
    <img src="https://img.shields.io/badge/처리_규모-5M+_레코드-orange?style=flat-square"/>
    <img src="https://img.shields.io/badge/포트-8081-blue?style=flat-square"/>
  </p>
</div>

---

## 📌 목차

1. [시스템 개요](#-시스템-개요)
2. [이 레포의 역할](#-이-레포의-역할--batch-server)
3. [기술 스택](#-기술-스택)
4. [배치 처리 흐름](#-배치-처리-흐름)
5. [환경 변수](#-환경-변수)
6. [시작하기](#-시작하기)
7. [API 문서](#-api-문서)
8. [관련 레포지토리](#-관련-레포지토리)
9. [Git 컨벤션](#-git-컨벤션)

---

## 🌐 시스템 개요

**AI Intelligence CRM** 은 대규모 상담 데이터를 AI로 분석하고 검색·관리할 수 있도록 설계된 **AI 기반 상담 기록 관리 시스템**입니다. 시스템은 역할에 따라 두 개의 백엔드 서버로 분리되어 운영됩니다.

| 서버 | 역할 | 포트 |
| :--- | :--- | :---: |
| **API Server** ([Ureca4_BE_api](https://github.com/4Ureca/Ureca4_BE_api)) | 실시간 사용자 요청 처리, 상담 조회·검색 API | `8080` |
| **Batch Server** (이 레포) | 대용량 상담 데이터 배치 처리, AI 분석, 검색 인덱싱 | `8081` |

---

## ⚙️ 이 레포의 역할 — Batch Server

이 서버는 **5M+ 건의 상담 레코드**를 안정적으로 처리하는 시스템의 핵심 데이터 처리 엔진입니다.

MySQL에 저장된 원천 상담 데이터를 **Chunk 기반 배치 Job**으로 읽어 들이고, **Gemini AI**로 의미 분석 후 **Elasticsearch·MongoDB**에 고속 인덱싱하는 전체 파이프라인을 담당합니다.

```
MySQL (원천 상담 데이터)
      │
      ▼ Spring Batch (Chunk 단위 읽기)
      │
      ▼ Gemini AI API (의미 분석 / 감정 분류 / 요약)
      │
      ├──▶ Elasticsearch  (전문 검색 인덱싱)
      └──▶ MongoDB        (분석 결과 문서 저장)
```

---

## 🛠 기술 스택

### Core

| 기술 | 버전 | 용도 |
| :--- | :---: | :--- |
| Java | 17 | 메인 언어 |
| Spring Boot | 3.5.11 | 애플리케이션 프레임워크 |
| Spring Batch | - | Chunk 기반 대용량 배치 처리, 재시도/스킵 정책 |
| Spring Retry + AOP | - | AI API 호출 실패 시 자동 재시도 |
| Spring Validation | - | 입력 데이터 유효성 검사 |
| Swagger (springdoc) | 2.8.6 | 배치 트리거 API 문서 자동화 |
| Datafaker | 2.2.2 | 대용량 테스트 데이터 생성 |
| Dotenv (`spring-dotenv`) | 4.0.0 | `.env` 파일 자동 로드 |
| Lombok | - | 보일러플레이트 코드 제거 |

### AI & 데이터 저장소

| 기술 | 용도 |
| :--- | :--- |
| **Gemini AI (Google)** | 상담 내용 의미 분석, 감정 분류, 요약 생성 |
| **MySQL** | 원천 상담 데이터 Read (Spring Batch ItemReader) |
| **Elasticsearch** | 상담 데이터 전문 검색 인덱싱 |
| **MongoDB** | AI 분석 결과 문서 저장 |
| **Redis** | 배치 상태 캐싱, 중복 처리 방지 |

### 인프라

| 기술 | 용도 |
| :--- | :--- |
| Docker | 컨테이너화 (eclipse-temurin:17-jre 기반) |
| G1GC (`-XX:+UseG1GC`) | 대용량 처리 시 GC 최적화 (힙 512MB ~ 2GB) |

---

## 🔄 배치 처리 흐름

배치 Job은 크게 **3단계 Step**으로 구성됩니다.

```
[Step 1] 상담 데이터 추출 (MySQL → Reader)
    │  JdbcPagingItemReader / JpaPagingItemReader
    │  Chunk Size 단위로 MySQL에서 상담 레코드 페이지 읽기
    ▼

[Step 2] AI 분석 및 변환 (Processor)
    │  Gemini AI API 호출 (Spring Retry 적용)
    │  - 상담 내용 의미 분석
    │  - 감정 상태 분류
    │  - 핵심 키워드 추출 및 요약 생성
    ▼

[Step 3] 다중 저장소 인덱싱 (Writer)
    ├──▶ Elasticsearch : 전문 검색용 인덱스 적재
    └──▶ MongoDB       : AI 분석 결과 문서 저장
```

**내결함성(Fault Tolerance) 전략**

| 전략 | 적용 내용 |
| :--- | :--- |
| Skip Policy | AI API 오류 또는 개별 레코드 변환 실패 시 해당 청크를 건너뛰고 계속 진행 |
| Retry Policy | Gemini API 호출 일시 실패 시 Spring Retry를 통해 자동 재시도 |
| JobRepository | 배치 메타데이터를 MySQL에 저장하여 중단 시 재시작 지원 |

---

## 🔐 환경 변수

`.env.example`을 복사해 프로젝트 루트에 `.env` 파일을 생성하고 아래 항목을 채워넣으세요.

> `spring-dotenv` 라이브러리가 적용되어 있어 `.env` 파일만 만들면 IDE 환경변수 설정 없이 자동으로 로드됩니다.

```bash
cp .env.example .env
```

| 변수명 | 설명 |
| :--- | :--- |
| `GEMINI_API_KEY` | Google Gemini AI API 키 — AI 분석 호출에 사용 |

> DB, Elasticsearch, MongoDB, Redis 연결 정보는 API 서버의 `docker` 디렉토리 내 `docker-compose.yml` 기준 기본값을 사용합니다. 별도 변경 시 `application.yml`을 수정하세요.

---

## 🚀 시작하기

### 사전 요구사항

- Java 17+
- Gradle
- **인프라 컨테이너 실행 필수** — [API 서버 레포](https://github.com/4Ureca/Ureca4_BE_api)의 `docker/` 폴더에서 먼저 실행해야 합니다.

### 인프라 실행 (API 서버 레포 기준)

```bash
# API 서버 레포 클론 후
cd docker
docker compose up -d
```

아래 컨테이너가 모두 정상 기동되어야 합니다.

| 컨테이너 | 용도 | 포트 |
| :--- | :--- | :---: |
| `crm-mysql` | 원천 상담 데이터 DB | `13306` |
| `crm-mongo` | AI 분석 결과 저장 | `27018` |
| `crm-redis` | 캐시 / 중복 처리 방지 | `6380` |
| `crm-es` | 전문 검색 인덱스 | `9201` |
| `crm-kibana` | ES 모니터링 UI | `15601` |
| `crm-mongo-ui` | MongoDB 관리 UI | `18081` |

### Batch 서버 실행

**1. 레포지토리 클론**

```bash
git clone https://github.com/CoderGogh/AI-Intelligence-CRM-BE-Batch.git
cd AI-Intelligence-CRM-BE-Batch
```

**2. 환경 변수 설정**

```bash
cp .env.example .env
# .env 파일에 GEMINI_API_KEY 값 입력
```

**3. 빌드 및 실행**

```bash
./gradlew build
./gradlew bootRun
```

**4. Docker 이미지 빌드 및 실행 (선택)**

```bash
./gradlew build
docker build -t crm-batch .
docker run -p 8081:8081 --env-file .env crm-batch
```

> Dockerfile은 `eclipse-temurin:17-jre` 기반이며, JVM 옵션 `-Xms512m -Xmx2048m -XX:+UseG1GC`가 적용되어 있습니다.

---

## 📖 API 문서

배치 Job을 수동으로 트리거하거나 상태를 조회하는 API는 Swagger UI에서 확인할 수 있습니다.

```
http://localhost:8081/swagger-ui.html
```

---

## 🔗 관련 레포지토리

| 레포 | 설명 |
| :--- | :--- |
| [4Ureca/Ureca4_BE_api](https://github.com/4Ureca/Ureca4_BE_api) | API 서버 — 실시간 상담 조회·검색 API, 인프라 Docker 구성 포함 |
| [CoderGogh/AI-Intelligence-CRM-BE-Batch](https://github.com/CoderGogh/AI-Intelligence-CRM-BE-Batch) | 이 레포 — 배치 처리 엔진 |

---

## 📐 Git 컨벤션

### 브랜치 전략

| 브랜치 | 설명 |
| :--- | :--- |
| `main` | 배포 대상 브랜치 |
| `develop` | 개발 통합 브랜치 |
| `feat/{feature-name}` | 기능 개발 브랜치 — develop으로 병합 |
| `hotfix/` | 버그 수정 브랜치 |
| `docs/` | 문서 작업 브랜치 |

**브랜치 예시**

```
feat/users
feat/consult
feat/jira에_등록된_기능_이름
```

### 커밋 타입

| 타입 | 설명 |
| :--- | :--- |
| `feat` | 새로운 기능 추가 또는 요구사항 반영 수정 |
| `fix` | 버그 수정 |
| `build` | 빌드 설정, 모듈 설치·삭제 |
| `chore` | 패키지 매니저, `.gitignore` 등 기타 수정 |
| `ci` | CI 설정 수정 |
| `docs` | 문서·주석 수정 |
| `style` | 코드 스타일·포맷팅 수정 |
| `refactor` | 기능 변화 없는 코드 리팩터링 |
| `test` | 테스트 코드 추가·수정 |
| `release` | 버전 릴리즈 |

**커밋 메시지 예시**

```
feat/홍길동/batch/상담데이터_elasticsearch_인덱싱_배치_구현
fix/홍길동/batch/gemini_api_재시도_로직_수정
```
