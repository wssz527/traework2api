package main

// 静态 HTML 样例：从 TRAE 官方更新日志页（2026-09-08 抓取）裁出的片段，
// 用于解析函数单测。内容不含任何凭证。

const fixtureChangelog = `date-FTRzTs">2026-09-01</div><div class="metaItemWrapper-aUE6IR"><div class="metaItem-Mgnq7u version-b47QNh">v<!-- -->3.3.93-96</div><span class="metaItem-Mgnq7u type-jZDufH">TraeCode</span></div></div></div><div class="contentColumn-UuFpBb"><div class="versionContent-wBRkfT"><ul class="unorderedList-OMS3Sf level0-SoIh5k"><li class="listItem-TtXUs9 level0-SoIh5k">Solo Agent 智能体和 Agent 智能体将合并为 Agent 智能体，支持在 IDE、SOLO 模式使用。Agent 智能体包含原Solo Agent 智能体、 Agent 智能体的能力集合，包括：支持 /goal、/plan、/spec 等内置命令，支持根据模型选择是否开启 Max 模式，支持选择是否使用 Auto Mode 模型，支持调用自定义智能体等。【仅企业版】</li><li class="listItem-TtXUs9 level0-SoIh5k">修复了已知问题。</li></ul></div></div></div></section><section class="versionEntry-WiA43L"><div class="timelineLayout-OoS9qY"><div class="timelineColumn-A1JvXE"><div class="timelineDot-YPruux"></div></div><div class="metadataColumn-Zs4Sja"><div class="metadata-re0Htd"><div class="metaItem-Mgnq7u date-FTRzTs">2026-08-21</div><div class="metaItemWrapper-aUE6IR"><div class="metaItem-Mgnq7u version-b47QNh">v<!-- -->0.1.49-52</div><span class="metaItem-Mgnq7u type-jZDufH">TraeWork</span></div></div></div><div class="contentColumn-UuFpBb"><div class="versionContent-wBRkfT"><ul class="unorderedList-OMS3Sf level0-SoIh5k"><li class="listItem-TtXUs9 level0-SoIh5k">上线「电脑控制」功能。</li><li class="listItem-TtXUs9 level0-SoIh5k">Design 模式支持图片编辑。</li><li class="listItem-TtXUs9 level0-SoIh5k">对话框可一键最小化为悬浮窗小标。</li><li class="listItem-TtXUs9 level0-SoIh5k">修复了已知问题。</li></ul></div></div></div></section><section class="versionEntry-WiA43L"><div class="timelineLayout-OoS9qY"><div class="timelineColumn-A1JvXE"><div class="timelineDot-YPruux"></div></div><div class="metadataColumn-Zs4Sja"><div class="metadata-re0Htd"><div class="metaItem-Mgnq7u date-FTRzTs">2026-08-20</div><div class="metaItemWrapper-aUE6IR"><div class="metaItem-Mgnq7u version-b47QNh">v<!-- -->3.3.87-92</div><span class="metaItem-Mgnq7u type-jZDufH">TraeCode</span></div></div></div><div class="contentColumn-UuFpBb"><div class="versionContent-wBRkfT"><ul class="unorderedList-OMS3Sf level0-SoIh5k"><li class="listItem-TtXUs9 level0-SoIh5k">支持 TRAE 移动端连接 TraeCode。</li><li class="listItem-TtXUs9 level0-SoIh5k">修复了已知问题。</li></ul></div></div></div></section><section class="versionEntry-WiA43L"><div class="timelineLayout-OoS9qY"><div class="timelineColumn-A1JvXE"><div class="timelineDot-YPruux"></div></div><div class="metadataColumn-Zs4Sja"><div class="metadata-re0Htd"><div class="metaItem-Mgnq7u date-FTRzTs">2026-08-18</div><div class="metaItemWrapper-aUE6IR"><div class="metaItem-Mgnq7u version-b47QNh">v<!-- -->0.0.17-0.0.18</div><span class="metaItem-Mgnq7u type-jZDufH">TRAE APP</span></div></div></div><div class="contentColumn-UuFpBb"><div class="versionContent-wBRkfT"><ul class="unorderedList-OMS3Sf level0-SoIh5k"><li class="listItem-TtXUs9 level0-SoIh5k">首页增加了「我的文件」入口，可查看新对话里生成的图片、视频、html 产物，可查看所有飞书文档</li><li class="listItem-TtXUs9 level0-SoIh5k">通过 “+”入口，引用“当前项目文件”、“我的文件”、“飞书文档”。快速找到你在当前项目、TRAE 所有任务、飞书中的文件产物</li><li class="listItem-TtXUs9 level0-SoIh5k">发送任务时，可以不选择文件夹直接发送任务</li><li class="listItem-TtXUs9 level0-SoIh5k">支持会话分享：分享方式包括链接，二维码，长图，系统分享等</li><li class="listItem-TtXUs9 level0-SoIh5k">新增反馈入口：可以在设置页中，找到「帮助与反馈」，点击即可提交你对 TRAE 的使用体验与产品建议</li></ul></div></div></div></section><section class="versionEntry-WiA43L"><div class="timelineLayout-OoS9qY"><div class="timelineColumn-A1JvXE"><div class="timelineDot-YPruux"></div></div><div class="metadataColumn-Zs4Sja"><div class="metadata-re0Htd"><div class="metaItem-Mgnq7u date-FTRzTs">2026-08-11</div><div class="metaItemWrapper-aUE6IR"><div class="metaItem-Mgnq7u version-b47QNh">v<!-- -->0.1.47-48</div><span class="metaItem-Mgnq7u type-jZDufH">TraeWork</span></div></div></div><div class="contentColumn-UuFpBb"><div class="versionContent-wBRkfT"><ul class="unorderedList-OMS3Sf level0-SoIh5k"><li class="listItem-TtXUs9 level0-SoIh5k">上线我的文件功能。</li><li class="listItem-TtXUs9 level0-SoIh5k">修复了已知问题。</li></ul></div></div></div></section><section class="versionEntry-WiA43L"><div class="timelineLayout-OoS9qY"><div class="timelineColumn-A1JvXE"><div class="timelineDot-YPruux"></div></div><div class="metadataColumn-Zs4Sja"><div class="metadata-re0Htd"><div class="metaItem-Mgnq7u `

const fixtureWorkChangelog = `<h2 id="huaPTCHgy" tabindex="-1">2026 年 08 月 21 日</h2>
<p>TraeWork 桌面版 v0.1.49 ~ 0.1.52 版本正式发布，TraeWork 网页版同步更新。以下是变更细节：</p>
<ul data-style="0">
<li>支持 “电脑控制” 功能。详情参考<a href="/work_computer-use" target="_blank">电脑控制（Computer Use）</a>。</li>
<li>支持编辑 Design 模式中生成的图片。详情参考<a href="/work_use-the-design-mode#hYRjU41dp" target="_blank">在画布中预览并管理设计成果</a>。</li>
<li>支持将对话框最小化为悬浮窗小标。</li>
<li>修复了已知问题。</li>
</ul>
<h2 id="hsg4AFDKF" tabindex="-1">2026 年 08 月 11 日</h2>
<p>TraeWork 桌面版 v0.1.47 ~ 0.1.48 版本正式发布，TraeWork 网页版同步更新。以下是变更细节：</p>
<ul data-style="0">
<li>上线 “我的文件” 功能。详情参考<a href="/work_my-space" target="_blank">产物空间</a>。</li>
<li>修复了已知问题。</li>
</ul>
<h2 id="hFhvF7rlU" tabindex="-1">2026 年 08 月 07 日</h2>
<p>TraeWork 桌面版 v0.1.44 ~ 0.1.46 版本正式发布，TraeWork 网页版同步更新。以下是变更细节：</p>
<ul data-style="0">
<li>支持对话分享功能。详情参考<a href="/work_trae-work-web-and-desktop-quickstart#hOjh74i3f" target="_blank">分享对话</a>。</li>
<li>灰度上线插件自动推荐功能。</li>
<li>输入框 / 、附件上传按钮整合进「+」按钮。</li>
<li>上线办公助理功能。详情参考<a href="/work_bot-assistant" target="_blank">办公助理</a>。</li>
</ul>
<p>TRAE 移动端 App v0.0.16 版本正式发布。以下是变更细节：</p>
<ul data-style="0">
<li>支持直接在对话流内预览视频产物。点击视频卡片即可预览。</li>
</ul>
<h2 id="hHM6DzqFE" tabindex="-1">2026 年 07 月 31 日</h2>
<p>TraeWork 桌面版 v0.1.40 ~ 0.1.43 版本正式发布，TraeWork 网页版同步更新。以下是变更细节：</p>
<ul data-style="0">
<li>上线以积分为核心的计费模式。详情参考<a href="/ide_plans-and-billing" target="_blank">套餐与计费</a>。</li>
<li>上线模板库。详情参考<a href="/work_templates" target="_blank">模板库</a>。</li>
<li>修复了已知问题。</li>
</ul>
<h2 id="hUPS5pApu" ta`
