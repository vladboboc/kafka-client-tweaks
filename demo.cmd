@echo off
rem Runs a chapter's demo by its number, from cmd.exe or PowerShell, without the -D quoting rules:
rem
rem   demo.cmd 02                              chapter 02 with its defaults   (PowerShell: .\demo.cmd 02)
rem   demo.cmd 02 records=5000 linger.ms=20    demo parameters and client properties, as in the chapter's "Run it"
rem   demo.cmd 15 --spring.kafka.producer.properties.linger.ms=50
rem   demo.cmd plain  /  demo.cmd spring       list the demos of part 1 / part 2
rem
rem Chapters 01-13 run in plain-clients (exec:java), 14-21 in spring-boot-kafka (spring-boot:run), 22 is the Spring
rem module's test suite. Same behaviour as the ./demo script for Git Bash, macOS and Linux.
setlocal
cd /d "%~dp0"

set "DEMO=%~1"
if "%DEMO%"=="" goto usage
if /i "%DEMO%"=="help" goto usage
if /i "%DEMO%"=="plain" goto list_plain
if /i "%DEMO%"=="spring" goto list_spring

rem Everything after the first argument, taken from %* because cmd splits "records=5000" at the "=".
set "ALL=%*"
call set "REST=%%ALL:*%DEMO%=%%"

set "MODULE=plain"
if /i "%DEMO:~0,7%"=="spring-" set "MODULE=spring"

rem A chapter number picks the module. "08" would be octal for set /a, so prefix a 1 and subtract 100.
echo %DEMO%| findstr /r "^[0-9][0-9]*$" >nul
if errorlevel 1 goto run
if "%DEMO:~1%"=="" set "DEMO=0%DEMO%"
set /a N=1%DEMO%-100
set "MODULE="
if %N% geq 1 if %N% leq 13 set "MODULE=plain"
if %N% geq 14 if %N% leq 21 set "MODULE=spring"
if %N% equ 22 set "MODULE=tests"
if "%MODULE%"=="" goto bad_chapter

:run
if "%MODULE%"=="tests" goto run_tests
if "%MODULE%"=="spring" goto run_spring
echo ./mvnw -q -pl plain-clients -am compile exec:java "-Dexec.args=%DEMO%%REST%"
call "%~dp0mvnw.cmd" -q -pl plain-clients -am compile exec:java "-Dexec.args=%DEMO%%REST%"
exit /b %errorlevel%

:run_spring
echo ./mvnw -q -pl spring-boot-kafka -am compile spring-boot:run "-Dspring-boot.run.arguments=%DEMO%%REST%"
call "%~dp0mvnw.cmd" -q -pl spring-boot-kafka -am compile spring-boot:run "-Dspring-boot.run.arguments=%DEMO%%REST%"
exit /b %errorlevel%

:run_tests
echo ./mvnw -q -pl spring-boot-kafka -am verify
call "%~dp0mvnw.cmd" -q -pl spring-boot-kafka -am verify
exit /b %errorlevel%

:list_plain
call "%~dp0mvnw.cmd" -q -pl plain-clients -am compile exec:java "-Dexec.args=list"
exit /b %errorlevel%

:list_spring
call "%~dp0mvnw.cmd" -q -pl spring-boot-kafka -am compile spring-boot:run
exit /b %errorlevel%

:bad_chapter
echo unknown chapter: %DEMO%
call :usage
exit /b 2

:usage
echo usage: demo.cmd ^<chapter 01-22 ^| demo name^> [key=value ...]
echo        demo.cmd plain    list the plain-client demos (chapters 01-13)
echo        demo.cmd spring   list the Spring Boot demos (chapters 14-22)
exit /b 0
