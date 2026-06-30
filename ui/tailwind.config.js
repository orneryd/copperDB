/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        'midnight': {
          'DEFAULT': '#060B12',
          'navy': '#0C1622',
          'graphite': '#1E2630',
        },
        'silver': {
          'DEFAULT': '#C7CDD6',
          'steel': '#9CA7B5',
          'structural': '#4B5563',
        },
        'copper': {
          'DEFAULT': '#B87333',
          'oxidized': '#C96A19',
          'bronze': '#8A4D1E',
        },
        'verdigris': {
          'DEFAULT': '#00B8C8',
          'teal': '#1F8E96',
          'deep': '#0E4F57',
        },
        'text': {
          'primary': '#F3F6FA',
          'secondary': '#B8C1CC',
          'muted': '#7D8896',
        },
      },
      fontFamily: {
        'mono': ['JetBrains Mono', 'Fira Code', 'monospace'],
        'display': ['Inter', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
