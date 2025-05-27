#!/bin/bash

# 定义构建目录
BUILD_DIR="build"

# 清理之前的构建文件
echo "Cleaning previous build (removing ${BUILD_DIR} directory)..."
if [ -d "${BUILD_DIR}" ]; then
  rm -rf "${BUILD_DIR}"
  if [ $? -ne 0 ]; then
    echo "Failed to remove ${BUILD_DIR}. Aborting."
    exit 1
  fi
fi

# 创建构建目录
echo "Creating build directory: ${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"
if [ $? -ne 0 ]; then
  echo "Failed to create ${BUILD_DIR}. Aborting."
  exit 1
fi

# 进入构建目录
cd "${BUILD_DIR}"
if [ $? -ne 0 ]; then
  echo "Failed to change directory to ${BUILD_DIR}. Aborting."
  exit 1
fi

# 重新构建项目
echo "Configuring CMake..."
cmake -DCMAKE_BUILD_TYPE=Debug ..

# 检查 CMake 配置是否成功
if [ $? -ne 0 ]; then
  echo "CMake configuration failed. Aborting."
  cd .. # 返回到项目根目录
  exit 1
fi

echo "Building project with CMake..."
cmake --build . -j$(nproc) # 使用所有可用的 CPU核心并行编译

# 检查构建是否成功
if [ $? -ne 0 ]; then
  echo "CMake build failed."
  cd .. # 返回到项目根目录
  exit 1
fi

# 返回到项目根目录
cd ..

echo "Build successful."
exit 0