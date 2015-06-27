################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/fileSystem.c 

OBJS += \
./src/fileSystem.o 

C_DEPS += \
./src/fileSystem.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I/usr/local/include/libbson-1.0 -I/usr/local/include/libmongoc-1.0 -O0 -g3 -Wall -c -fmessage-length=0 $(pkg-config --cflags libmongoc-1.0) -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


