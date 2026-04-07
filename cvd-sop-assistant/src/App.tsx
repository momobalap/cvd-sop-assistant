import { useState, useRef, useEffect } from 'react'
import { Send, Bot, User, Sparkles, Database, FileText, Zap } from 'lucide-react'

interface Message {
  id: string
  role: 'user' | 'assistant'
  content: string
  sources?: string[]
  isLoading?: boolean
}

const API_BASE = '/api'

async function sendQuery(question: string): Promise<{ answer: string; sources: string[] }> {
  const res = await fetch(`${API_BASE}/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ question }),
  })
  if (!res.ok) throw new Error('API Error')
  return res.json()
}

function App() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  useEffect(() => {
    scrollToBottom()
  }, [messages])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!input.trim() || isLoading) return

    const userMessage: Message = {
      id: Date.now().toString(),
      role: 'user',
      content: input.trim(),
    }

    setMessages(prev => [...prev, userMessage])
    setInput('')
    setIsLoading(true)

    // Add loading message
    const loadingId = (Date.now() + 1).toString()
    setMessages(prev => [...prev, {
      id: loadingId,
      role: 'assistant',
      content: '',
      isLoading: true,
    }])

    try {
      const result = await sendQuery(userMessage.content)
      setMessages(prev => prev.map(m => 
        m.id === loadingId 
          ? { ...m, content: result.answer, sources: result.sources, isLoading: false }
          : m
      ))
    } catch (err) {
      setMessages(prev => prev.map(m => 
        m.id === loadingId 
          ? { ...m, content: '抱歉，查询时出现错误。请稍后再试。', isLoading: false }
          : m
      ))
    } finally {
      setIsLoading(false)
    }
  }

  const suggestedQuestions = [
    'RF_VDC 异常的原因是什么？',
    'Diffuser 导致哪些异常？',
    '薄膜机台异常处理整体流程',
    'FC07 Particle 异常怎么处理？',
  ]

  return (
    <div className="min-h-screen flex flex-col bg-grid bg-glow">
      {/* Header */}
      <header className="border-b border-white/[0.08] bg-black/30 backdrop-blur-xl">
        <div className="max-w-4xl mx-auto px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-cyan-400 to-teal-500 flex items-center justify-center">
              <Zap className="w-5 h-5 text-black" />
            </div>
            <div>
              <h1 className="font-semibold text-lg tracking-tight">CVD SOP 智能助手</h1>
              <p className="text-xs text-white/40">薄膜 & CVD 异常处理知识库</p>
            </div>
          </div>
          <div className="flex items-center gap-2 text-xs text-white/40">
            <div className="w-2 h-2 rounded-full bg-green-400 animate-pulse" />
            <span>Neo4j + RAG</span>
          </div>
        </div>
      </header>

      {/* Messages */}
      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-8">
          {messages.length === 0 && (
            <div className="text-center py-16 animate-fade-in-up">
              <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-gradient-to-br from-cyan-500/20 to-purple-500/20 border border-cyan-500/20 mb-6">
                <Bot className="w-8 h-8 text-cyan-400" />
              </div>
              <h2 className="text-2xl font-semibold mb-2">有什么可以帮你的？</h2>
              <p className="text-white/40 mb-8 max-w-md mx-auto">
                询问 CVD 或薄膜机台异常处理相关问题，我会从图谱和 SOP 文档中为你查找答案。
              </p>
              
              {/* Suggested questions */}
              <div className="flex flex-wrap gap-2 justify-center max-w-2xl mx-auto">
                {suggestedQuestions.map((q, i) => (
                  <button
                    key={i}
                    onClick={() => setInput(q)}
                    className="glass-card px-4 py-2 text-sm text-white/60 hover:text-white hover:border-cyan-500/30 transition-all cursor-pointer"
                  >
                    {q}
                  </button>
                ))}
              </div>

              {/* Features */}
              <div className="flex gap-6 justify-center mt-12">
                <div className="flex items-center gap-2 text-xs text-white/30">
                  <Database className="w-4 h-4" />
                  <span>Neo4j 图谱</span>
                </div>
                <div className="flex items-center gap-2 text-xs text-white/30">
                  <FileText className="w-4 h-4" />
                  <span>SOP 文档</span>
                </div>
                <div className="flex items-center gap-2 text-xs text-white/30">
                  <Sparkles className="w-4 h-4" />
                  <span>AI 汇总</span>
                </div>
              </div>
            </div>
          )}

          {messages.map((msg) => (
            <div
              key={msg.id}
              className={`mb-6 animate-fade-in-up ${
                msg.role === 'user' ? 'flex justify-end' : ''
              }`}
            >
              <div className={`flex gap-3 ${msg.role === 'user' ? 'flex-row-reverse' : ''}`}>
                {/* Avatar */}
                <div className={`flex-shrink-0 w-8 h-8 rounded-lg flex items-center justify-center ${
                  msg.role === 'user'
                    ? 'bg-gradient-to-br from-cyan-400 to-teal-500'
                    : 'bg-white/5 border border-white/10'
                }`}>
                  {msg.role === 'user' 
                    ? <User className="w-4 h-4 text-black" />
                    : <Bot className="w-4 h-4 text-cyan-400" />
                  }
                </div>

                {/* Content */}
                <div className={`max-w-[75%] ${msg.role === 'user' ? 'items-end' : ''}`}>
                  <div className={`rounded-2xl px-5 py-4 ${
                    msg.role === 'user'
                      ? 'bg-gradient-to-br from-cyan-500 to-teal-500 text-black rounded-tr-sm'
                      : 'glass-card rounded-tl-sm'
                  }`}>
                    {msg.isLoading ? (
                      <div className="typing-indicator flex items-center gap-1 py-1">
                        <span></span>
                        <span></span>
                        <span></span>
                      </div>
                    ) : (
                      <div className="whitespace-pre-wrap text-sm leading-relaxed">
                        {msg.content}
                      </div>
                    )}
                  </div>

                  {/* Source badges */}
                  {msg.sources && msg.sources.length > 0 && !msg.isLoading && (
                    <div className="flex gap-2 mt-2">
                      {msg.sources.map((source, i) => (
                        <span
                          key={i}
                          className={`text-xs px-2 py-0.5 rounded-full badge-${
                            source.includes('Neo4j') ? 'neo4j' : source.includes('SOP') ? 'rag' : 'both'
                          }`}
                        >
                          {source}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </div>
          ))}
          <div ref={messagesEndRef} />
        </div>
      </main>

      {/* Input */}
      <footer className="border-t border-white/[0.08] bg-black/30 backdrop-blur-xl">
        <div className="max-w-4xl mx-auto px-6 py-4">
          <form onSubmit={handleSubmit} className="relative">
            <input
              type="text"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="输入你的问题..."
              disabled={isLoading}
              className="w-full bg-white/5 border border-white/10 rounded-xl px-5 py-4 pr-14 text-sm
                         placeholder:text-white/30 focus:outline-none input-glow transition-all
                         disabled:opacity-50"
            />
            <button
              type="submit"
              disabled={!input.trim() || isLoading}
              className="absolute right-2 top-1/2 -translate-y-1/2 w-10 h-10 rounded-lg
                         bg-gradient-to-br from-cyan-400 to-teal-500 flex items-center justify-center
                         disabled:opacity-30 disabled:cursor-not-allowed hover:scale-105 transition-transform"
            >
              <Send className="w-4 h-4 text-black" />
            </button>
          </form>
          <p className="text-center text-xs text-white/20 mt-3">
            基于 Neo4j 知识图谱和 SOP 文档 · 仅供参考
          </p>
        </div>
      </footer>
    </div>
  )
}

export default App
