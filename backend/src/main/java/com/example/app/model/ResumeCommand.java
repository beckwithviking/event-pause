package com.example.app.model;

public record ResumeCommand(String action) {
    public static ResumeCommand resume() {
        return new ResumeCommand("RESUME");
    }

    public boolean isResume() {
        return "RESUME".equals(action);
    }
}
