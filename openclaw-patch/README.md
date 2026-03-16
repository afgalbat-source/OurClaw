# OpenClaw 内置 TTS 语音气泡兼容性补丁说明

## 概述

本补丁用于修复 OpenClaw 内置 `tts` 在语音气泡场景下的兼容性问题。

本次修复主要面向以下语音气泡类消息通道：

- Feishu
- Telegram
- WhatsApp

在目标运行环境中，OpenClaw 原先为语音气泡场景默认选择的 Edge TTS 输出格式 `ogg-24khz-16bit-mono-opus` 会被上游 Edge 在线 TTS 服务拒绝，进而导致语音生成失败。实际表现包括：

- WebSocket 合成会话被上游服务关闭
- 本地生成的音频文件大小为 `0 bytes`
- 上层流程拿到无效音频文件路径，影响后续语音发送

本补丁将语音气泡场景的默认 Edge 输出格式调整为 `webm-24khz-16bit-mono-opus`。该格式已经在相同环境下完成验证，可以正常生成有效音频。

## 补丁文件

本补丁包包含以下两个文件：

- `tts.ts`
- `tts.test.ts`

## 目标文件位置

请将本目录中的补丁文件复制到目标 OpenClaw 源码树中的如下位置：

- `openclaw-patch/tts.ts` -> `openclaw/src/tts/tts.ts`
- `openclaw-patch/tts.test.ts` -> `openclaw/src/tts/tts.test.ts`

本补丁制作时所对应的绝对路径如下：

- `/inspire/qb-ilm/project/cq-scientific-cooperation-zone/ky26017/SciHarness-teamclaw/openclaw/src/tts/tts.ts`
- `/inspire/qb-ilm/project/cq-scientific-cooperation-zone/ky26017/SciHarness-teamclaw/openclaw/src/tts/tts.test.ts`

## 修复的问题

修复前，OpenClaw 在 Feishu、Telegram、WhatsApp 这类语音气泡通道中，会强制将 Edge TTS 输出格式设置为：

- `ogg-24khz-16bit-mono-opus`

但在当前目标环境下，上游 Edge 在线 TTS 服务并不接受该格式。我们在实际排查中观察到：

- WebSocket 会话建立成功，但服务端会主动关闭连接
- 关闭原因明确指向该输出格式不被支持
- 本地会留下 `0 bytes` 的空音频文件
- 上层逻辑继续将该空文件作为成功结果处理，导致后续语音发送异常

## 变更内容

### 1. 运行时默认行为调整

文件：

- `tts.ts`

修改内容：

- 更新常量 `VOICE_BUBBLE_EDGE_OUTPUT_FORMAT`
- 修改前：`ogg-24khz-16bit-mono-opus`
- 修改后：`webm-24khz-16bit-mono-opus`

变更效果：

- Feishu、Telegram、WhatsApp 这类语音气泡场景下，内置 `tts` 默认使用 `webm-24khz-16bit-mono-opus`
- 避开当前环境中已确认不兼容的 `ogg-24khz-16bit-mono-opus`

### 2. 测试用例同步更新

文件：

- `tts.test.ts`

修改内容：

- 将语音气泡场景默认输出格式的测试期望值同步更新
- 修改前：`ogg-24khz-16bit-mono-opus`
- 修改后：`webm-24khz-16bit-mono-opus`

变更效果：

- 自动化测试与新的运行时默认行为保持一致

## 与修改前源文件的差异

相对于修复前的源文件，本补丁仅包含以下两类改动：

- `tts.ts` 中一处运行时行为变更：将语音气泡场景的默认 Edge 输出格式由 Ogg/Opus 切换为 WebM/Opus
- `tts.test.ts` 中对应测试期望值的同步调整

除上述内容外，本补丁不包含其他功能性改动。

## 验证情况

本补丁已通过以下方式验证：

- 在目标环境中手动验证 Edge TTS 不同输出格式的实际行为
- 确认 `webm-24khz-16bit-mono-opus` 可以正常生成有效音频
- 确认 `ogg-24khz-16bit-mono-opus` 在相同环境下会被上游服务拒绝
- 运行自动化测试：

```bash
pnpm vitest run src/tts/tts.test.ts
```

验证结果：

- `1` 个测试文件通过
- `30` 个测试用例全部通过

## 备注

在本补丁包整理完成时，这两个补丁文件已经与当前工作中的 OpenClaw 源码目标文件保持一致。

因此，本目录的用途是：

- 作为独立发布的补丁包，便于复制到其他 OpenClaw 代码树
- 作为本次修复内容的归档说明，便于外部使用者理解本次修改的目的和差异
